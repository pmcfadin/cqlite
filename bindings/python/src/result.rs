//! Query result types for Python bindings.
//!
//! This module provides Python wrappers for query execution results:
//! - `QueryResult` - Container for rows, metadata, and timing
//! - `Row` - Individual row with dict-like access
//! - `ColumnInfo` - Column metadata
//! - `QueryResultIter` - Iterator for Pythonic for-loops
//! - `StreamingIterator` - Memory-efficient streaming iterator for large result sets

use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::to_py_err;
use crate::runtime::block_on;
use crate::value::{key_error, value_to_py};

/// Query result from execute().
///
/// Contains rows, metadata, and execution timing.
/// Supports iteration: `for row in result:`
///
/// # Attributes
///
/// * `rows` - List of Row objects
/// * `rows_affected` - Number of rows affected (for DML)
/// * `execution_time_ms` - Query execution time in milliseconds
/// * `columns` - List of ColumnInfo metadata
///
/// # Example
///
/// ```python
/// result = db.execute("SELECT * FROM users LIMIT 10")
/// print(f"Got {len(result)} rows in {result.execution_time_ms}ms")
/// for row in result:
///     print(row["name"])
/// ```
#[pyclass(module = "cqlite")]
pub struct QueryResult {
    /// Converted rows (using Py<Row> for GIL-independence)
    rows: Vec<Py<Row>>,
    /// Number of rows affected (for DML operations)
    rows_affected: u64,
    /// Execution time in milliseconds
    execution_time_ms: u64,
    /// Column metadata
    columns: Vec<ColumnInfo>,
}

#[pymethods]
impl QueryResult {
    /// Get all rows as a list.
    #[getter]
    fn rows(&self, py: Python<'_>) -> Vec<Py<Row>> {
        self.rows.iter().map(|r| r.clone_ref(py)).collect()
    }

    /// Number of rows affected (INSERT/UPDATE/DELETE).
    #[getter]
    fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// Query execution time in milliseconds.
    #[getter]
    fn execution_time_ms(&self) -> u64 {
        self.execution_time_ms
    }

    /// Column information for the result set.
    #[getter]
    fn columns(&self) -> Vec<ColumnInfo> {
        self.columns.clone()
    }

    /// Number of rows in result.
    fn __len__(&self) -> usize {
        self.rows.len()
    }

    /// Make result iterable: `for row in result:`
    fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> QueryResultIter {
        QueryResultIter {
            rows: slf.rows.iter().map(|r| r.clone_ref(py)).collect(),
            index: 0,
        }
    }

    /// Convert entire result to Python dict.
    ///
    /// Returns a dict with keys: rows, rows_affected, execution_time_ms, columns
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new(py);

        // Convert rows to list of dicts
        let rows_list: Vec<PyObject> = self
            .rows
            .iter()
            .map(|row| row.borrow(py).to_dict(py))
            .collect::<PyResult<Vec<_>>>()?;
        dict.set_item("rows", PyList::new(py, rows_list)?)?;

        // Add metadata
        dict.set_item("rows_affected", self.rows_affected)?;
        dict.set_item("execution_time_ms", self.execution_time_ms)?;

        // Convert columns
        let columns_list: Vec<PyObject> = self
            .columns
            .iter()
            .map(|col| col.to_dict(py))
            .collect::<PyResult<Vec<_>>>()?;
        dict.set_item("columns", PyList::new(py, columns_list)?)?;

        Ok(dict.into_any().unbind())
    }

    /// String representation.
    fn __repr__(&self) -> String {
        format!(
            "QueryResult(rows={}, rows_affected={}, execution_time_ms={})",
            self.rows.len(),
            self.rows_affected,
            self.execution_time_ms
        )
    }
}

impl QueryResult {
    /// Number of materialised rows (for SELECT spans, issue #1039).
    pub(crate) fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Number of rows affected (for DML spans, issue #1039).
    pub(crate) fn rows_affected_value(&self) -> i64 {
        self.rows_affected as i64
    }

    /// Create a minimal QueryResult for a DML operation (INSERT/UPDATE/DELETE).
    ///
    /// The result has no rows but reflects the number of mutations applied.
    pub(crate) fn from_write(
        _py: Python<'_>,
        rows_affected: u64,
        execution_time_ms: u64,
    ) -> PyResult<Self> {
        Ok(Self {
            rows: Vec::new(),
            rows_affected,
            execution_time_ms,
            columns: Vec::new(),
        })
    }

    /// Convert from cqlite_core QueryResult.
    ///
    /// Eagerly converts all rows and values to Python objects.
    /// This is done within the GIL to ensure thread safety.
    pub(crate) fn from_core(
        py: Python<'_>,
        result: cqlite_core::query::result::QueryResult,
    ) -> PyResult<Self> {
        // Convert columns metadata
        let columns: Vec<ColumnInfo> = result
            .metadata
            .columns
            .iter()
            .map(ColumnInfo::from_core)
            .collect();

        // Build the shared column ordering + name index ONCE for the whole
        // result (issue #1445), from the authoritative SELECT order. Every row
        // shares it by reference-count instead of re-cloning column-name
        // Strings per row.
        let shape = build_row_shape(py, &result.metadata.columns);

        // Convert all rows eagerly and wrap in Py<Row>
        let rows: Vec<Py<Row>> = result
            .rows
            .iter()
            .map(|row| {
                let row_obj = Row::from_core(py, row, shape.clone())?;
                Py::new(py, row_obj)
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(Self {
            rows,
            rows_affected: result.rows_affected,
            execution_time_ms: result.execution_time_ms,
            columns,
        })
    }
}

/// Iterator over QueryResult rows.
///
/// Enables Pythonic iteration: `for row in result:`
#[pyclass(module = "cqlite")]
pub struct QueryResultIter {
    rows: Vec<Py<Row>>,
    index: usize,
}

#[pymethods]
impl QueryResultIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> Option<Py<Row>> {
        if slf.index < slf.rows.len() {
            let row = slf.rows[slf.index].clone_ref(py);
            slf.index += 1;
            Some(row)
        } else {
            None
        }
    }
}

/// Column ordering + name index shared by every `Row` in one result (issue #1445).
///
/// Built **once per result** from `metadata.columns` (the authoritative SELECT
/// order) and shared by reference-count (`Arc`) with every row, so a wide-table
/// scan no longer re-clones each column-name `String` per row. `keys` drives the
/// SELECT-ordered iteration surface (`keys()`/`items()`/`to_dict()`); `index`
/// gives O(1) name→position lookup for `__getitem__`/`get`/`__contains__`.
#[derive(Clone)]
struct RowShape {
    /// Interned column-name handles in SELECT order.
    keys: Arc<[Py<PyString>]>,
    /// name -> position in `keys`, for O(1) lookup by column name.
    index: Arc<HashMap<String, usize>>,
}

/// Build the shared per-result row shape from core column metadata.
///
/// `columns` is the authoritative SELECT order (`QueryMetadata.columns`); the
/// name `String`s and `PyString` handles are allocated exactly once here.
fn build_row_shape(py: Python<'_>, columns: &[cqlite_core::query::result::ColumnInfo]) -> RowShape {
    let keys: Arc<[Py<PyString>]> = columns
        .iter()
        .map(|c| PyString::new(py, &c.name).unbind())
        .collect();
    let index: HashMap<String, usize> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.clone(), i))
        .collect();
    RowShape {
        keys,
        index: Arc::new(index),
    }
}

/// Build the shared row shape from a `QueryRow`'s value keys.
///
/// Used by the streaming path when `metadata.columns` is empty (schema-less
/// `SELECT *`): core's `get_result_columns()` can return no columns even though
/// the streamed rows carry values. Keys are sorted alphabetically to mirror the
/// materialized/core ordering (see `select_executor::execute`, issue #129/#140,
/// which populates `metadata.columns` from the first row's sorted `values`
/// keys), so streaming output matches materialized output.
fn build_row_shape_from_row_keys(
    py: Python<'_>,
    row: &cqlite_core::query::result::QueryRow,
) -> RowShape {
    let mut names: Vec<&str> = row.values.keys().map(|k| k.as_ref()).collect();
    names.sort_unstable();
    let keys: Arc<[Py<PyString>]> = names
        .iter()
        .map(|n| PyString::new(py, n).unbind())
        .collect();
    let index: HashMap<String, usize> = names
        .iter()
        .enumerate()
        .map(|(i, n)| ((*n).to_string(), i))
        .collect();
    RowShape {
        keys,
        index: Arc::new(index),
    }
}

/// A single row from query results with dict-like access.
///
/// Supports both dict-style indexing and method access:
/// - `row["column_name"]` - Get value by column name
/// - `row.get("column", default)` - Get with fallback
/// - `row.keys()` - Get column names
/// - `"column" in row` - Check if column exists
///
/// Columns are returned in **SELECT order** (the order of `result.columns`),
/// matching the CLI's ordered JSON output (issue #1445).
///
/// # Example
///
/// ```python
/// row = result.rows[0]
/// name = row["name"]
/// age = row.get("age", 0)
/// for key in row.keys():
///     print(f"{key}: {row[key]}")
/// ```
#[pyclass(module = "cqlite")]
pub struct Row {
    /// Column ordering + name→position index, shared by every row in the result.
    shape: RowShape,
    /// Positional values aligned to `shape.keys` (SELECT order).
    values: Vec<PyObject>,
}

#[pymethods]
impl Row {
    /// Dict-style access: `row["column_name"]`
    ///
    /// Raises KeyError if column doesn't exist. O(1) via the shared name index.
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<PyObject> {
        self.shape
            .index
            .get(key)
            .map(|&pos| self.values[pos].clone_ref(py))
            .ok_or_else(|| key_error(key))
    }

    /// Membership test: `"column" in row`
    fn __contains__(&self, key: &str) -> bool {
        self.shape.index.contains_key(key)
    }

    /// Get all column names, in SELECT order.
    fn keys(&self, py: Python<'_>) -> Vec<String> {
        self.shape
            .keys
            .iter()
            .map(|k| k.bind(py).to_string())
            .collect()
    }

    /// Get all values, in SELECT order.
    fn values(&self, py: Python<'_>) -> Vec<PyObject> {
        self.values.iter().map(|v| v.clone_ref(py)).collect()
    }

    /// Get all (key, value) pairs, in SELECT order.
    fn items(&self, py: Python<'_>) -> Vec<(String, PyObject)> {
        self.shape
            .keys
            .iter()
            .zip(self.values.iter())
            .map(|(k, v)| (k.bind(py).to_string(), v.clone_ref(py)))
            .collect()
    }

    /// Convert to Python dict, in SELECT order.
    ///
    /// `PyDict` preserves insertion order, so the returned dict iterates in
    /// SELECT order.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        for (key, value) in self.shape.keys.iter().zip(self.values.iter()) {
            dict.set_item(key.bind(py), value.clone_ref(py))?;
        }
        Ok(dict.into_any().unbind())
    }

    /// Get value with default fallback.
    ///
    /// Returns the value for key if it exists, otherwise returns default.
    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<PyObject>) -> PyObject {
        self.shape
            .index
            .get(key)
            .map(|&pos| self.values[pos].clone_ref(py))
            .unwrap_or_else(|| default.unwrap_or_else(|| py.None()))
    }

    /// Number of columns.
    fn __len__(&self) -> usize {
        self.shape.keys.len()
    }

    /// String representation (columns shown in SELECT order).
    fn __repr__(&self, py: Python<'_>) -> String {
        let keys: Vec<String> = self
            .shape
            .keys
            .iter()
            .map(|k| k.bind(py).to_string())
            .collect();
        if keys.len() <= 5 {
            format!("Row({{{}}})", keys.join(", "))
        } else {
            format!(
                "Row({{{}, ... and {} more}})",
                keys[..5].join(", "),
                keys.len() - 5
            )
        }
    }
}

impl Row {
    /// Convert from a core `QueryRow`, placing values positionally per `shape`.
    ///
    /// `shape` carries the SELECT order + name index shared across the whole
    /// result (built once by [`build_row_shape`]); values are placed by name
    /// lookup so the row is correct even if the core row's `HashMap` iteration
    /// order differs from column order. A column present in `shape` but absent
    /// from the row becomes Python `None`.
    pub(crate) fn from_core(
        py: Python<'_>,
        row: &cqlite_core::query::result::QueryRow,
        shape: RowShape,
    ) -> PyResult<Self> {
        let mut slots: Vec<Option<PyObject>> = (0..shape.keys.len()).map(|_| None).collect();
        for (name, value) in &row.values {
            if let Some(&pos) = shape.index.get(name.as_ref()) {
                slots[pos] = Some(value_to_py(py, value)?);
            }
        }
        let values = slots
            .into_iter()
            .map(|v| v.unwrap_or_else(|| py.None()))
            .collect();
        Ok(Self { shape, values })
    }
}

/// Column metadata for result set.
///
/// Provides information about each column in the query result.
///
/// # Attributes
///
/// * `name` - Column name
/// * `data_type` - CQL data type as string (e.g., "text", "int")
/// * `nullable` - Whether column can be null
/// * `position` - Column position (0-indexed)
/// * `table_name` - Original table name (for joined queries)
#[pyclass(module = "cqlite")]
#[derive(Clone)]
pub struct ColumnInfo {
    /// Column name
    #[pyo3(get)]
    name: String,
    /// CQL data type as string
    #[pyo3(get)]
    data_type: String,
    /// Whether column can be null
    #[pyo3(get)]
    nullable: bool,
    /// Column position in result set
    #[pyo3(get)]
    position: usize,
    /// Original table name (for joined queries)
    #[pyo3(get)]
    table_name: Option<String>,
}

#[pymethods]
impl ColumnInfo {
    /// String representation.
    fn __repr__(&self) -> String {
        format!(
            "ColumnInfo(name='{}', data_type='{}', nullable={}, position={})",
            self.name, self.data_type, self.nullable, self.position
        )
    }

    /// Convert to Python dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("name", &self.name)?;
        dict.set_item("data_type", &self.data_type)?;
        dict.set_item("nullable", self.nullable)?;
        dict.set_item("position", self.position)?;
        if let Some(ref table_name) = self.table_name {
            dict.set_item("table_name", table_name)?;
        }
        Ok(dict.into_any().unbind())
    }
}

impl ColumnInfo {
    /// Convert from cqlite_core ColumnInfo.
    fn from_core(col: &cqlite_core::query::result::ColumnInfo) -> Self {
        Self {
            name: col.name.clone(),
            data_type: format!("{:?}", col.data_type),
            nullable: col.nullable,
            position: col.position,
            table_name: col.table_name.clone(),
        }
    }
}

/// Streaming iterator for memory-efficient query results.
///
/// Yields rows one at a time from a streaming query, keeping memory usage
/// bounded by the `StreamingConfig` settings. Use this for large result sets
/// that would not fit in memory.
///
/// # Resource Cleanup
///
/// The iterator holds a Tokio channel receiver connected to a background
/// producer task. Resources are cleaned up automatically when:
///
/// 1. The iterator is fully consumed (`StopIteration` raised)
/// 2. The iterator is garbage collected (Python `__del__`)
/// 3. The producer task completes (channel sender dropped)
///
/// Early termination via `break` is safe - the channel and any remaining
/// buffered rows are cleaned up when the iterator is dropped.
///
/// # Thread Safety
///
/// The iterator uses internal locking and is safe to use from a single
/// Python thread. However, sharing a `StreamingIterator` between threads
/// is not recommended - instead, create separate iterators per thread.
///
/// # Example
///
/// ```python
/// config = cqlite.StreamingConfig(buffer_size=512)
/// for row in db.execute_streaming("SELECT * FROM large_table", config=config):
///     process(row)
///     # Memory stays bounded; only buffer_size rows in flight
///
/// # Early termination is safe
/// for row in db.execute_streaming("SELECT * FROM large_table"):
///     if row["id"] == target:
///         break  # Resources cleaned up automatically
/// ```
#[pyclass(module = "cqlite")]
pub struct StreamingIterator {
    /// The wrapped core iterator (Mutex for interior mutability without &mut self)
    inner: Mutex<cqlite_core::query::result::QueryResultIterator>,
    /// Per-call observability span (`python.execute_streaming`, issue #1039).
    ///
    /// The span is kept alive for the whole iteration so the streamed rows are
    /// attributed to the caller's trace. The total rows yielded is recorded into
    /// the span's `cqlite.rows` field exactly once, when the stream finalizes —
    /// whichever comes first of: normal exhaustion (`StopIteration` from
    /// `__next__`), or `Drop` (garbage collection / early `break`).
    ///
    /// Wrapped in `Mutex<Option<..>>` so it can be finalized idempotently from
    /// `&self` (`__next__`) as well as `&mut self` (`Drop`): the first finalize
    /// `.take()`s the span (recording the count and ending it), and any later
    /// finalize is a no-op. This guarantees that a fully-exhausted-but-still-
    /// referenced iterator has already exported its span before a later
    /// `Database.close()` flushes telemetry.
    span: Mutex<Option<tracing::Span>>,
    /// Shared per-stream row shape (SELECT order + name index), built lazily on
    /// the first `__next__` from the iterator's `metadata.columns` and reused by
    /// every streamed row so column names are interned once (issue #1445).
    row_shape: Mutex<Option<RowShape>>,
}

impl StreamingIterator {
    /// Create a new streaming iterator from a core QueryResultIterator with no
    /// observability span (used where instrumentation is not wired).
    pub fn new(iter: cqlite_core::query::result::QueryResultIterator) -> Self {
        Self::with_span(iter, tracing::Span::none())
    }

    /// Create a streaming iterator that records into `span` as rows are yielded.
    pub fn with_span(
        iter: cqlite_core::query::result::QueryResultIterator,
        span: tracing::Span,
    ) -> Self {
        Self {
            inner: Mutex::new(iter),
            span: Mutex::new(Some(span)),
            row_shape: Mutex::new(None),
        }
    }

    /// Finalize the streaming span exactly once.
    ///
    /// Records the authoritative total row count into `cqlite.rows` and ends the
    /// span by dropping it. Called from both the exhaustion branch of `__next__`
    /// and from `Drop`; the `.take()` makes the second caller a no-op, so the
    /// count is never recorded twice. Poisoned locks are skipped rather than
    /// risking a double panic.
    /// Return the shared row shape for this stream, building it once from the
    /// iterator's `metadata.columns` (the authoritative SELECT order) and
    /// caching it for every subsequent row (issue #1445).
    ///
    /// When `metadata.columns` is empty — core's streaming `get_result_columns()`
    /// returns no columns for schema-less `SELECT *` even though the streamed rows
    /// carry values — the shape is instead built from `first_row`'s value keys
    /// (sorted, matching the materialized path) so rows don't lose all their
    /// values (issue #1445 streaming regression).
    fn row_shape(
        &self,
        py: Python<'_>,
        iter: &cqlite_core::query::result::QueryResultIterator,
        first_row: &cqlite_core::query::result::QueryRow,
    ) -> PyResult<RowShape> {
        let mut slot = self
            .row_shape
            .lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Row shape lock poisoned"))?;
        if let Some(shape) = slot.as_ref() {
            return Ok(shape.clone());
        }
        let shape = if iter.metadata.columns.is_empty() {
            build_row_shape_from_row_keys(py, first_row)
        } else {
            build_row_shape(py, &iter.metadata.columns)
        };
        *slot = Some(shape.clone());
        Ok(shape)
    }

    fn finalize_span(&self) {
        let Ok(mut span_slot) = self.span.lock() else {
            return;
        };
        if let Some(span) = span_slot.take() {
            if let Ok(iter) = self.inner.lock() {
                span.record("cqlite.rows", iter.rows_received() as i64);
            }
            // Dropping `span` here ends it so the span (with its final row count)
            // is exported now, before any later Database.close()/flush.
        }
    }
}

impl Drop for StreamingIterator {
    fn drop(&mut self) {
        // Finalize the span if it has not already been finalized at exhaustion
        // (i.e. the iterator was GC'd or abandoned via `break`). Idempotent: if
        // `__next__` already finalized on StopIteration, this is a no-op.
        self.finalize_span();
    }
}

#[pymethods]
impl StreamingIterator {
    /// Return self for iteration.
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Get the next row from the stream.
    ///
    /// Raises StopIteration when no more rows are available.
    fn __next__(&self, py: Python<'_>) -> PyResult<Py<Row>> {
        // Lock must be held across the async operation because next_async()
        // mutates the iterator's internal state (rows_received counter and
        // channel receiver). This is safe because Python's GIL ensures only
        // one Python thread accesses this iterator at a time. The block_on
        // call waits on a bounded channel receive, so lock contention is minimal.
        let mut iter = self
            .inner
            .lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Iterator lock poisoned"))?;

        let next_result = block_on(iter.next_async());

        match next_result {
            Some(Ok(row)) => {
                // Convert core row to Python Row, sharing the per-stream ordered
                // shape (built once) so column names are interned per stream, not
                // per row, and columns stay in SELECT order (issue #1445).
                let shape = self.row_shape(py, &iter, &row)?;
                let py_row = Row::from_core(py, &row, shape)?;
                Py::new(py, py_row)
            }
            Some(Err(e)) => Err(to_py_err(e)),
            None => {
                // Stream exhausted. Finalize the span now (idempotently) so the
                // per-stream span and its final row count are exported even if
                // the exhausted iterator stays alive until a later
                // Database.close()/flush. Release the `inner` lock first because
                // finalize_span() re-acquires it.
                drop(iter);
                self.finalize_span();
                Err(PyStopIteration::new_err(()))
            }
        }
    }

    /// Get the number of rows received so far.
    ///
    /// Useful for progress tracking when total is known.
    #[getter]
    fn rows_received(&self) -> PyResult<u64> {
        let iter = self
            .inner
            .lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Iterator lock poisoned"))?;
        Ok(iter.rows_received())
    }

    /// Get the progress percentage (if total is known).
    ///
    /// Returns None if the total row count is not available.
    #[getter]
    fn progress_percent(&self) -> PyResult<Option<f64>> {
        let iter = self
            .inner
            .lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Iterator lock poisoned"))?;
        Ok(iter.progress_percent())
    }

    /// String representation.
    fn __repr__(&self) -> String {
        if let Ok(iter) = self.inner.lock() {
            format!("StreamingIterator(rows_received={})", iter.rows_received())
        } else {
            "StreamingIterator(locked)".to_string()
        }
    }
}

/// Register result types with the Python module.
pub fn register_result(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<QueryResult>()?;
    m.add_class::<QueryResultIter>()?;
    m.add_class::<Row>()?;
    m.add_class::<ColumnInfo>()?;
    m.add_class::<StreamingIterator>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_info_creation() {
        let col = ColumnInfo {
            name: "test_col".to_string(),
            data_type: "Text".to_string(),
            nullable: true,
            position: 0,
            table_name: None,
        };
        assert_eq!(col.name, "test_col");
        assert_eq!(col.data_type, "Text");
        assert!(col.nullable);
        assert_eq!(col.position, 0);
    }
}
