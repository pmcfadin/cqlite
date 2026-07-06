//! DatabaseStats wrapper for Python bindings.
//!
//! This module provides the `DatabaseStats` class for Python access
//! to CQLite's database statistics functionality.

use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Database statistics.
///
/// DatabaseStats provides comprehensive statistics about the database
/// including storage metrics, memory usage, and query statistics.
/// Created via `Database.stats()`.
///
/// # Properties
///
/// - `storage_stats`: SSTable storage metrics (count, size, entries)
/// - `memory_stats`: Memory and cache statistics
/// - `query_stats`: Query execution statistics (when state_machine enabled)
///
/// # Example
///
/// ```python
/// stats = db.stats()
/// print(f"SSTables: {stats.storage_stats['sstable_count']}")
/// print(f"Cache hits: {stats.memory_stats['block_cache_hits']}")
/// print(f"All stats: {stats.to_dict()}")
/// ```
#[pyclass(module = "cqlite")]
pub struct DatabaseStats {
    storage_stats: Py<PyDict>,
    memory_stats: Py<PyDict>,
    query_stats: Option<Py<PyDict>>,
}

impl DatabaseStats {
    /// Create DatabaseStats from core DatabaseStats.
    pub fn from_core(py: Python<'_>, stats: cqlite_core::DatabaseStats) -> PyResult<Self> {
        // Convert storage stats
        let storage = PyDict::new(py);
        let sstables = &stats.storage_stats.sstables;
        storage.set_item("sstable_count", sstables.sstable_count)?;
        storage.set_item("total_size", sstables.total_size)?;
        storage.set_item("total_entries", sstables.total_entries)?;
        storage.set_item("total_tables", sstables.total_tables)?;
        storage.set_item("average_size", sstables.average_size)?;

        // Convert memory stats
        let memory = PyDict::new(py);
        let mem = &stats.memory_stats;
        memory.set_item("block_cache_hits", mem.block_cache_hits)?;
        memory.set_item("block_cache_misses", mem.block_cache_misses)?;
        // Issue #1571 (B5): honest cache observability — real chunk-cache
        // evictions/capacity and the aggregated key-cache counters.
        memory.set_item("block_cache_evictions", mem.block_cache_evictions)?;
        memory.set_item("block_cache_capacity_bytes", mem.block_cache_capacity_bytes)?;
        memory.set_item("key_cache_hits", mem.key_cache_hits)?;
        memory.set_item("key_cache_misses", mem.key_cache_misses)?;
        memory.set_item("key_cache_evictions", mem.key_cache_evictions)?;
        memory.set_item("key_cache_resident_bytes", mem.key_cache_resident_bytes)?;
        memory.set_item("key_cache_capacity_bytes", mem.key_cache_capacity_bytes)?;
        memory.set_item("row_cache_hits", mem.row_cache_hits)?;
        memory.set_item("row_cache_misses", mem.row_cache_misses)?;
        memory.set_item("total_memory_used", mem.total_memory_used)?;
        memory.set_item("buffer_allocations", mem.buffer_allocations)?;
        memory.set_item("buffer_deallocations", mem.buffer_deallocations)?;

        // Convert query stats (state_machine feature is always enabled in Python bindings)
        let query_stats = {
            let query = PyDict::new(py);
            let q = &stats.query_stats;
            query.set_item("total_queries", q.total_queries)?;
            query.set_item("error_queries", q.error_queries)?;
            query.set_item("avg_execution_time_us", q.avg_execution_time_us)?;
            query.set_item("cache_hit_ratio", q.cache_hit_ratio)?;
            query.set_item("rows_affected", q.rows_affected)?;
            Some(query.into())
        };

        Ok(Self {
            storage_stats: storage.into(),
            memory_stats: memory.into(),
            query_stats,
        })
    }
}

#[pymethods]
impl DatabaseStats {
    /// SSTable storage statistics.
    ///
    /// Returns a dictionary containing:
    /// - `sstable_count`: Number of SSTable files
    /// - `total_size`: Total size in bytes
    /// - `total_entries`: Total number of entries
    /// - `total_tables`: Number of tables
    /// - `average_size`: Average SSTable size in bytes
    #[getter]
    fn storage_stats(&self, py: Python<'_>) -> Py<PyDict> {
        self.storage_stats.clone_ref(py)
    }

    /// Memory and cache statistics.
    ///
    /// Returns a dictionary containing:
    /// - `block_cache_hits`: Number of block (decompressed-chunk) cache hits
    /// - `block_cache_misses`: Number of block cache misses
    /// - `block_cache_evictions`: Chunk-cache entries evicted to stay within budget (B5, #1571)
    /// - `block_cache_capacity_bytes`: Configured chunk-cache byte budget (B5, #1571)
    /// - `key_cache_hits`: Aggregate key→partition-offset cache hits (B5, #1571)
    /// - `key_cache_misses`: Aggregate key-cache misses (B5, #1571)
    /// - `key_cache_evictions`: Aggregate key-cache evictions (B5, #1571)
    /// - `key_cache_resident_bytes`: Aggregate key-cache resident bytes (B5, #1571)
    /// - `key_cache_capacity_bytes`: Aggregate key-cache byte budget (B5, #1571)
    /// - `row_cache_hits`: Number of row cache hits
    /// - `row_cache_misses`: Number of row cache misses
    /// - `total_memory_used`: Total memory used in bytes (chunk-cache resident bytes)
    /// - `buffer_allocations`: Number of buffer allocations
    /// - `buffer_deallocations`: Number of buffer deallocations
    #[getter]
    fn memory_stats(&self, py: Python<'_>) -> Py<PyDict> {
        self.memory_stats.clone_ref(py)
    }

    /// Query execution statistics.
    ///
    /// Returns a dictionary containing (when available):
    /// - `total_queries`: Total queries executed
    /// - `error_queries`: Number of queries that errored
    /// - `avg_execution_time_us`: Average execution time in microseconds
    /// - `cache_hit_ratio`: Query cache hit ratio (0.0 to 1.0)
    /// - `rows_affected`: Total rows affected
    ///
    /// Returns None if query statistics are not available.
    #[getter]
    fn query_stats(&self, py: Python<'_>) -> Option<Py<PyDict>> {
        self.query_stats.as_ref().map(|q| q.clone_ref(py))
    }

    /// Convert all statistics to a dictionary.
    ///
    /// Returns a nested dictionary containing all available statistics.
    ///
    /// # Example
    ///
    /// ```python
    /// d = stats.to_dict()
    /// print(d['storage_stats']['sstable_count'])
    /// print(d['memory_stats']['total_memory_used'])
    /// ```
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("storage_stats", self.storage_stats.clone_ref(py))?;
        dict.set_item("memory_stats", self.memory_stats.clone_ref(py))?;
        if let Some(ref q) = self.query_stats {
            dict.set_item("query_stats", q.clone_ref(py))?;
        }
        Ok(dict.into())
    }

    /// String representation of database statistics.
    fn __repr__(&self) -> String {
        "DatabaseStats(...)".to_string()
    }
}

/// Register database stats types with the Python module.
pub fn register_stats(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DatabaseStats>()?;
    Ok(())
}
