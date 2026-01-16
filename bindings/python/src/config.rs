//! Configuration bridge for Python bindings.
//!
//! Bridges Python configuration dicts/JSON to `cqlite_core::Config`.
//!
//! # Supported Configuration Methods
//!
//! 1. **Python dict**: Nested dict matching core Config structure
//! 2. **JSON string**: Parsed to Config via serde
//! 3. **Preset string**: `"memory_optimized"` or `"performance_optimized"`
//! 4. **None**: Uses default configuration
//!
//! # Example
//!
//! ```python
//! # Using StreamingConfig
//! config = cqlite.StreamingConfig(buffer_size=2048, chunk_size=5000)
//!
//! # Using preset
//! db = cqlite.open(path, config="memory_optimized")
//!
//! # Using dict
//! db = cqlite.open(path, config={"memory": {"max_memory": 64 * 1024 * 1024}})
//! ```

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Configuration for streaming query execution.
///
/// Controls memory usage during large result set iteration.
///
/// # Attributes
///
/// * `buffer_size` - Number of rows to buffer in flight (default: 1024)
/// * `chunk_size` - Number of rows per fetch chunk (default: 10,000)
///
/// # Example
///
/// ```python
/// config = cqlite.StreamingConfig(buffer_size=512, chunk_size=1000)
/// for row in db.execute_streaming("SELECT * FROM large_table", config=config):
///     process(row)
/// ```
#[pyclass(module = "cqlite")]
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Number of rows to buffer in memory during streaming (default: 1024).
    #[pyo3(get, set)]
    pub buffer_size: usize,

    /// Number of rows to fetch per chunk from storage (default: 10,000).
    #[pyo3(get, set)]
    pub chunk_size: usize,
}

#[pymethods]
impl StreamingConfig {
    /// Create a new StreamingConfig with optional parameters.
    ///
    /// # Arguments
    ///
    /// * `buffer_size` - Rows in flight buffer (default: 1024, must be > 0)
    /// * `chunk_size` - Rows per fetch chunk (default: 10,000, must be > 0)
    ///
    /// # Returns
    ///
    /// A new StreamingConfig instance
    ///
    /// # Raises
    ///
    /// * `ValueError` - If buffer_size or chunk_size is 0
    #[new]
    #[pyo3(signature = (buffer_size=1024, chunk_size=10_000))]
    fn new(buffer_size: usize, chunk_size: usize) -> PyResult<Self> {
        if buffer_size == 0 {
            return Err(PyValueError::new_err("buffer_size must be greater than 0"));
        }
        if chunk_size == 0 {
            return Err(PyValueError::new_err("chunk_size must be greater than 0"));
        }
        Ok(StreamingConfig {
            buffer_size,
            chunk_size,
        })
    }

    /// Returns a string representation of the config.
    fn __repr__(&self) -> String {
        format!(
            "StreamingConfig(buffer_size={}, chunk_size={})",
            self.buffer_size, self.chunk_size
        )
    }
}

impl Default for StreamingConfig {
    fn default() -> Self {
        StreamingConfig {
            buffer_size: 1024,
            chunk_size: 10_000,
        }
    }
}

/// Returns a memory-optimized configuration preset as a Python dict.
///
/// This preset minimizes memory usage at the cost of some performance:
/// - max_memory: 256 MB
/// - Aggressive compression (Zstd)
/// - Smaller caches and buffers
///
/// # Example
///
/// ```python
/// config = cqlite.memory_optimized()
/// db = cqlite.open(path, config=config)
/// ```
#[pyfunction]
pub fn memory_optimized(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let config = cqlite_core::Config::memory_optimized();
    config_to_py_dict(py, &config)
}

/// Returns a performance-optimized configuration preset as a Python dict.
///
/// This preset maximizes performance at the cost of higher memory usage:
/// - max_memory: 4 GB
/// - Fast compression (LZ4)
/// - Larger caches and more I/O threads
///
/// # Example
///
/// ```python
/// config = cqlite.performance_optimized()
/// db = cqlite.open(path, config=config)
/// ```
#[pyfunction]
pub fn performance_optimized(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let config = cqlite_core::Config::performance_optimized();
    config_to_py_dict(py, &config)
}

/// Validates a configuration dict or JSON string.
///
/// # Arguments
///
/// * `config` - A Python dict or JSON string representing the configuration
///
/// # Returns
///
/// `True` if valid, raises `ValueError` if invalid
///
/// # Raises
///
/// * `ValueError` - If the configuration is invalid
///
/// # Example
///
/// ```python
/// config = {"memory": {"max_memory": 0}}  # Invalid!
/// cqlite.validate_config(config)  # Raises ValueError
/// ```
#[pyfunction]
pub fn validate_config(py: Python<'_>, config: &Bound<'_, PyAny>) -> PyResult<bool> {
    let core_config = config_from_py(py, Some(config))?;
    core_config
        .validate()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(true)
}

/// Parse a Python configuration value into a core Config.
///
/// The resulting config is automatically validated before returning.
///
/// # Arguments
///
/// * `config` - One of:
///   - `None`: Returns default config
///   - `dict`: Parsed via JSON bridge
///   - `str` (JSON): Parsed directly
///   - `str` (preset): `"memory_optimized"` or `"performance_optimized"`
///
/// # Returns
///
/// A validated `cqlite_core::Config` instance
///
/// # Errors
///
/// Returns `PyValueError` if:
/// - JSON parsing fails
/// - Dict conversion fails
/// - Unknown preset name
/// - Configuration validation fails
pub fn config_from_py(
    py: Python<'_>,
    config: Option<&Bound<'_, PyAny>>,
) -> PyResult<cqlite_core::Config> {
    let core_config = match config {
        None => cqlite_core::Config::default(),
        Some(obj) => {
            // Check if it's a string (JSON or preset name)
            if let Ok(s) = obj.extract::<String>() {
                config_from_string(&s)?
            } else if let Ok(dict) = obj.downcast::<PyDict>() {
                // Check if it's a dict
                config_from_dict(py, dict)?
            } else {
                return Err(PyValueError::new_err(
                    "config must be a dict, JSON string, or preset name ('memory_optimized', 'performance_optimized')",
                ));
            }
        }
    };

    // Validate the parsed config before returning
    core_config
        .validate()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    Ok(core_config)
}

/// Parse a string as either a preset name or JSON config.
fn config_from_string(s: &str) -> PyResult<cqlite_core::Config> {
    // Check for preset names
    match s {
        "memory_optimized" => Ok(cqlite_core::Config::memory_optimized()),
        "performance_optimized" => Ok(cqlite_core::Config::performance_optimized()),
        _ => {
            // Try parsing as JSON
            serde_json::from_str(s).map_err(|e| {
                PyValueError::new_err(format!(
                    "Invalid config: not a preset name and invalid JSON: {}",
                    e
                ))
            })
        }
    }
}

/// Convert a Python dict to Config via JSON bridge.
fn config_from_dict(py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<cqlite_core::Config> {
    // Use Python's json module to serialize the dict
    let json_module = py.import("json")?;
    let json_str: String = json_module.call_method1("dumps", (dict,))?.extract()?;

    // Parse JSON to Config
    serde_json::from_str(&json_str)
        .map_err(|e| PyValueError::new_err(format!("Invalid config dict: {}", e)))
}

/// Convert a core Config to a Python dict via JSON bridge.
fn config_to_py_dict(py: Python<'_>, config: &cqlite_core::Config) -> PyResult<Py<PyDict>> {
    // Serialize config to JSON
    let json_str = serde_json::to_string(config)
        .map_err(|e| PyValueError::new_err(format!("Failed to serialize config: {}", e)))?;

    // Use Python's json module to parse back to dict
    let json_module = py.import("json")?;
    let dict = json_module.call_method1("loads", (json_str,))?;

    // Downcast and return owned reference
    let py_dict = dict.downcast::<PyDict>()?;
    Ok(py_dict.clone().unbind())
}

/// Register configuration-related items with the Python module.
pub fn register_config(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<StreamingConfig>()?;
    m.add_function(wrap_pyfunction!(memory_optimized, m)?)?;
    m.add_function(wrap_pyfunction!(performance_optimized, m)?)?;
    m.add_function(wrap_pyfunction!(validate_config, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_config_defaults() {
        let config = StreamingConfig::default();
        assert_eq!(config.buffer_size, 1024);
        assert_eq!(config.chunk_size, 10_000);
    }

    #[test]
    fn test_streaming_config_custom_values() {
        let config = StreamingConfig::new(512, 5000).unwrap();
        assert_eq!(config.buffer_size, 512);
        assert_eq!(config.chunk_size, 5000);
    }

    #[test]
    fn test_streaming_config_repr() {
        let config = StreamingConfig::new(1024, 10_000).unwrap();
        assert_eq!(
            config.__repr__(),
            "StreamingConfig(buffer_size=1024, chunk_size=10000)"
        );
    }

    #[test]
    fn test_streaming_config_zero_buffer_size_fails() {
        let result = StreamingConfig::new(0, 10_000);
        assert!(result.is_err());
    }

    #[test]
    fn test_streaming_config_zero_chunk_size_fails() {
        let result = StreamingConfig::new(1024, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_from_preset_memory_optimized() {
        let config = config_from_string("memory_optimized").unwrap();
        assert_eq!(config.memory.max_memory, 256 * 1024 * 1024);
    }

    #[test]
    fn test_config_from_preset_performance_optimized() {
        let config = config_from_string("performance_optimized").unwrap();
        assert_eq!(config.memory.max_memory, 4 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_config_from_invalid_preset() {
        let result = config_from_string("invalid_preset");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not a preset name"));
    }

    #[test]
    fn test_config_from_json_string() {
        let json = r#"{"memory": {"max_memory": 134217728}}"#;
        let config = config_from_string(json).unwrap();
        assert_eq!(config.memory.max_memory, 128 * 1024 * 1024);
    }

    #[test]
    fn test_config_from_invalid_json() {
        let result = config_from_string("{invalid json}");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid JSON"));
    }
}
