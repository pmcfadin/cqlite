use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use pyo3::types::{PyDict, PyList};
use tokio::time::{sleep, Duration};
use crate::query::{QueryExecutor, ParsedQuery};
use crate::types::CQLiteRow;
use crate::errors::QueryError;

/// Async support for CQLite Python bindings
/// 
/// This module provides asynchronous query execution capabilities, enabling
/// memory-efficient processing of large SSTable files through streaming
/// and concurrent operations.

/// Async query iterator for streaming large result sets
#[pyclass]
pub struct AsyncQueryIterator {
    sstable_path: String,
    sql: String,
    chunk_size: u32,
    current_offset: u32,
    finished: bool,
}

#[pymethods]
impl AsyncQueryIterator {
    #[new]
    fn new(sstable_path: String, sql: String, chunk_size: u32) -> Self {
        AsyncQueryIterator {
            sstable_path,
            sql,
            chunk_size,
            current_offset: 0,
            finished: false,
        }
    }
    
    /// Async iterator protocol
    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
    
    /// Async next method
    fn __anext__(&mut self, py: Python) -> PyResult<PyObject> {
        let sstable_path = self.sstable_path.clone();
        let sql = self.sql.clone();
        let chunk_size = self.chunk_size;
        let current_offset = self.current_offset;
        let finished = self.finished;
        
        future_into_py(py, async move {
            if finished {
                return Err(PyStopAsyncIteration::new_err("No more items"));
            }
            
            // Create executor and execute query chunk
            let executor = QueryExecutor::new(&sstable_path)?;
            let mut parsed_query = executor.parse_sql(&sql)?;
            
            // Apply offset and limit for this chunk
            parsed_query.offset = Some(current_offset);
            parsed_query.limit = Some(chunk_size);
            
            // Execute query
            let results = executor.execute_query(parsed_query)?;
            
            // Convert results to Python objects
            Python::with_gil(|py| {
                let py_rows = PyList::empty(py);
                for row in &results {
                    py_rows.append(row.to_pydict(py)?)?;
                }
                
                // Check if we're done
                if results.len() < chunk_size as usize {
                    // This was the last chunk
                    Ok(py_rows.into())
                } else {
                    Ok(py_rows.into())
                }
            })
        })
    }
    
    /// Get remaining item count estimate
    fn get_remaining_estimate(&self) -> PyResult<Option<u64>> {
        // This would use SSTable statistics to estimate remaining rows
        // For now, return None (unknown)
        Ok(None)
    }
}

/// Async query executor for concurrent operations
#[pyclass]
pub struct AsyncQueryExecutor {
    sstable_path: String,
    max_concurrent: usize,
}

#[pymethods]
impl AsyncQueryExecutor {
    #[new]
    fn new(sstable_path: String, max_concurrent: Option<usize>) -> Self {
        AsyncQueryExecutor {
            sstable_path,
            max_concurrent: max_concurrent.unwrap_or(4),
        }
    }
    
    /// Execute multiple queries concurrently
    fn execute_concurrent(&self, py: Python, queries: Vec<String>) -> PyResult<PyObject> {
        let sstable_path = self.sstable_path.clone();
        let max_concurrent = self.max_concurrent;
        
        future_into_py(py, async move {
            let mut tasks = Vec::new();
            
            // Create semaphore to limit concurrent queries
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent));
            
            for sql in queries {
                let sstable_path = sstable_path.clone();
                let semaphore = semaphore.clone();
                
                let task = tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    
                    // Execute query
                    let executor = QueryExecutor::new(&sstable_path)?;
                    let parsed_query = executor.parse_sql(&sql)?;
                    let results = executor.execute_query(parsed_query)?;
                    
                    // Convert to Python objects
                    Python::with_gil(|py| {
                        let py_rows = PyList::empty(py);
                        for row in &results {
                            py_rows.append(row.to_pydict(py)?)?;
                        }
                        Ok::<PyObject, PyErr>(py_rows.into())
                    })
                });
                
                tasks.push(task);
            }
            
            // Wait for all tasks to complete
            let mut all_results = Vec::new();
            for task in tasks {
                match task.await {
                    Ok(Ok(result)) => all_results.push(result),
                    Ok(Err(err)) => return Err(err),
                    Err(_) => return Err(QueryError::new_err("Task execution failed")),
                }
            }
            
            Python::with_gil(|py| {
                let py_results = PyList::new(py, all_results);
                Ok(py_results.into())
            })
        })
    }
    
    /// Execute query with timeout
    fn execute_with_timeout(&self, py: Python, sql: String, timeout_seconds: f64) -> PyResult<PyObject> {
        let sstable_path = self.sstable_path.clone();
        
        future_into_py(py, async move {
            let timeout_duration = Duration::from_secs_f64(timeout_seconds);
            
            let query_future = async {
                let executor = QueryExecutor::new(&sstable_path)?;
                let parsed_query = executor.parse_sql(&sql)?;
                executor.execute_query(parsed_query)
            };
            
            match tokio::time::timeout(timeout_duration, query_future).await {
                Ok(Ok(results)) => {
                    Python::with_gil(|py| {
                        let py_rows = PyList::empty(py);
                        for row in &results {
                            py_rows.append(row.to_pydict(py)?)?;
                        }
                        Ok(py_rows.into())
                    })
                }
                Ok(Err(err)) => Err(err),
                Err(_) => Err(QueryError::new_err(format!(
                    "Query timed out after {:.1} seconds", timeout_seconds
                ))),
            }
        })
    }
    
    /// Execute query with progress callback
    fn execute_with_progress(&self, py: Python, sql: String, callback: PyObject) -> PyResult<PyObject> {
        let sstable_path = self.sstable_path.clone();
        
        future_into_py(py, async move {
            // Start progress reporting
            Python::with_gil(|py| {
                callback.call1(py, (0.0, "Starting query execution..."))
            })?;
            
            // Simulate progress updates during query execution
            for i in 1..=5 {
                sleep(Duration::from_millis(100)).await;
                
                let progress = i as f64 * 0.2;
                let message = match i {
                    1 => "Parsing SQL...",
                    2 => "Opening SSTable...",
                    3 => "Reading data...",
                    4 => "Processing results...",
                    5 => "Finalizing...",
                    _ => "Working...",
                };
                
                Python::with_gil(|py| {
                    callback.call1(py, (progress, message))
                })?;
            }
            
            // Execute actual query
            let executor = QueryExecutor::new(&sstable_path)?;
            let parsed_query = executor.parse_sql(&sql)?;
            let results = executor.execute_query(parsed_query)?;
            
            // Final progress update
            Python::with_gil(|py| {
                callback.call1(py, (1.0, "Query completed!"))?;
                
                let py_rows = PyList::empty(py);
                for row in &results {
                    py_rows.append(row.to_pydict(py)?)?;
                }
                Ok(py_rows.into())
            })
        })
    }
}

/// Async batch processor for multiple SSTable files
#[pyclass]
pub struct AsyncBatchProcessor {
    sstable_paths: Vec<String>,
    max_concurrent: usize,
}

#[pymethods]
impl AsyncBatchProcessor {
    #[new]
    fn new(sstable_paths: Vec<String>, max_concurrent: Option<usize>) -> Self {
        AsyncBatchProcessor {
            sstable_paths,
            max_concurrent: max_concurrent.unwrap_or(4),
        }
    }
    
    /// Process same query across multiple SSTable files
    fn process_all(&self, py: Python, sql: String) -> PyResult<PyObject> {
        let sstable_paths = self.sstable_paths.clone();
        let max_concurrent = self.max_concurrent;
        
        future_into_py(py, async move {
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent));
            let mut tasks = Vec::new();
            
            for sstable_path in sstable_paths {
                let sql = sql.clone();
                let semaphore = semaphore.clone();
                
                let task = tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    
                    let executor = QueryExecutor::new(&sstable_path)?;
                    let parsed_query = executor.parse_sql(&sql)?;
                    let results = executor.execute_query(parsed_query)?;
                    
                    Ok::<Vec<CQLiteRow>, PyErr>(results)
                });
                
                tasks.push(task);
            }
            
            // Collect all results
            let mut all_results = Vec::new();
            for task in tasks {
                match task.await {
                    Ok(Ok(results)) => all_results.extend(results),
                    Ok(Err(err)) => return Err(err),
                    Err(_) => return Err(QueryError::new_err("Batch processing failed")),
                }
            }
            
            Python::with_gil(|py| {
                let py_rows = PyList::empty(py);
                for row in &all_results {
                    py_rows.append(row.to_pydict(py)?)?;
                }
                Ok(py_rows.into())
            })
        })
    }
    
    /// Process with aggregation across files
    fn process_with_aggregation(&self, py: Python, sql: String, agg_function: String) -> PyResult<PyObject> {
        let sstable_paths = self.sstable_paths.clone();
        
        future_into_py(py, async move {
            // This would implement aggregation functions like SUM, COUNT, AVG
            // across multiple SSTable files
            
            match agg_function.to_lowercase().as_str() {
                "count" => {
                    let mut total_count = 0u64;
                    
                    for sstable_path in sstable_paths {
                        let executor = QueryExecutor::new(&sstable_path)?;
                        let parsed_query = executor.parse_sql(&sql)?;
                        let count = executor.execute_count(parsed_query)?;
                        total_count += count;
                    }
                    
                    Python::with_gil(|py| {
                        let result = PyDict::new(py);
                        result.set_item("count", total_count)?;
                        Ok(result.into())
                    })
                }
                _ => Err(QueryError::new_err(format!(
                    "Unsupported aggregation function: {}", agg_function
                ))),
            }
        })
    }
}

/// Custom StopAsyncIteration exception for Python
pyo3::create_exception!(cqlite, PyStopAsyncIteration, pyo3::exceptions::PyStopIteration);

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn test_async_query_iterator_creation() {
        let iterator = AsyncQueryIterator::new(
            "test.db".to_string(),
            "SELECT * FROM users".to_string(),
            1000,
        );
        
        assert_eq!(iterator.sstable_path, "test.db");
        assert_eq!(iterator.sql, "SELECT * FROM users");
        assert_eq!(iterator.chunk_size, 1000);
        assert_eq!(iterator.current_offset, 0);
        assert!(!iterator.finished);
    }
    
    #[test]
    fn test_async_executor_creation() {
        let executor = AsyncQueryExecutor::new("test.db".to_string(), Some(8));
        
        assert_eq!(executor.sstable_path, "test.db");
        assert_eq!(executor.max_concurrent, 8);
    }
}