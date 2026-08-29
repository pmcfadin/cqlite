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

impl StreamingConfig {
    /// Convert to core StreamingConfig for use with cqlite_core.
    pub fn to_core(&self) -> cqlite_core::query::result::StreamingConfig {
        cqlite_core::query::result::StreamingConfig {
            buffer_size: self.buffer_size,
            chunk_size: self.chunk_size,
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

/// Parse a Python configuration value into a core Config, then validate it.
///
/// The resulting config is automatically validated before returning. A caller
/// that must fold an override in BEFORE validation wants
/// [`parse_config_from_py`] instead.
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
    let (core_config, removed_key_warning) = parse_config_from_py(py, config)?;

    // Validate the parsed config before returning
    core_config
        .validate()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    // ONLY now — the warning asserts the configuration still loads, and this
    // operation has only just finished deciding that (#1696 roborev r2 F2).
    raise_removed_key_warning(py, removed_key_warning)?;

    Ok(core_config)
}

/// Parse a Python configuration value into a core Config WITHOUT validating it,
/// returning it alongside any REMOVED-key warning the document earned.
///
/// Split out of [`config_from_py`] for the one caller that must fold a
/// documented override into the config *before* it is judged (issue #1697,
/// roborev r2): `cqlite.open`'s `flush_threshold` replaces
/// `storage.memtable_size_threshold`, so validating the base first rejected a
/// config that was invalid ONLY in the field the override was about to replace —
/// a merged config that would have been perfectly valid.
///
/// Every other caller wants [`config_from_py`], which validates. This returns an
/// UNVALIDATED config, so the caller owns validating the config it finally uses;
/// returning one that is never validated is a bug.
///
/// # The warning is RETURNED, not raised (#1696 roborev r2 F2)
///
/// This function used to raise it here, which reintroduced on the Python surface
/// the exact defect fixed for the CLI in F3: a document naming a removed key AND
/// carrying an invalid surviving value warned "the configuration still loads" and
/// then the public operation REJECTED it. So the warning travels with the config
/// and the caller raises it only once its own validation has SUCCEEDED — which is
/// the moment "still loads" becomes true.
pub fn parse_config_from_py(
    py: Python<'_>,
    config: Option<&Bound<'_, PyAny>>,
) -> PyResult<(cqlite_core::Config, Option<String>)> {
    let parsed = match config {
        None => (cqlite_core::Config::default(), None),
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

    Ok(parsed)
}

/// Assemble the public `Config` for `cqlite.open`: parse it, fold the optional
/// `flush_threshold` override into it, then validate the MERGED result.
///
/// The ORDER is the point (#1697 roborev r2). Validation used to run on the BASE
/// config, before the fold, so a config invalid ONLY in the field the override
/// was about to replace was rejected even though the merged config was valid —
/// e.g. a dict whose `storage.memtable_size_threshold` sits above the caller's
/// `memtable_hard_limit`, passed together with a `flush_threshold` that brings it
/// back under.
///
/// The override is folded into the config ITSELF rather than a clone handed to
/// the write engine: after #1697 the public config is the single source of truth,
/// so the read side must see the same effective threshold the engine runs on.
///
/// NOTE: `config_from_dict` deserializes into the full `cqlite_core::Config`,
/// which is NOT `#[serde(default)]`, so `config` must be a COMPLETE config — a
/// full dict, a full JSON string, or a preset; a partial dict is rejected with
/// missing-field errors. To flip one switch, take a full dict from a preset
/// (e.g. `cqlite.performance_optimized()`), set
/// `["storage"]["compaction"]["auto_compaction"] = False`, then pass it.
pub fn config_for_open(
    py: Python<'_>,
    config: Option<&Bound<'_, PyAny>>,
    flush_threshold: Option<u64>,
) -> PyResult<cqlite_core::Config> {
    let (mut core_config, removed_key_warning) = parse_config_from_py(py, config)?;

    if let Some(v) = flush_threshold {
        // The ceiling check MUST compare against the CALLER's
        // `memtable_hard_limit` — never `Config::default()`'s (#1697 roborev r1).
        // Above the caller's ceiling, auto-flush never fires and admission
        // rejects first: the write path dead-ends permanently (roborev 2885).
        // Kept alongside the merged `validate()` below because it names the
        // OVERRIDE and both operands, where the core error names only the two
        // config fields.
        let hard_limit = core_config.storage.memtable_hard_limit;
        if v > hard_limit {
            return Err(PyValueError::new_err(format!(
                "flush_threshold ({v} bytes) must not exceed the memtable hard limit ({hard_limit} bytes)"
            )));
        }
        core_config.storage.memtable_size_threshold = v;
    }

    core_config
        .validate()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    // Raised LAST, after the override fold and the merged validation both
    // succeeded: every earlier `return Err` above leaves the caller with the
    // rejection and no "the configuration still loads" assurance beside it
    // (#1696 roborev r2 F2).
    raise_removed_key_warning(py, removed_key_warning)?;

    Ok(core_config)
}

/// Parse a string as either a preset name or JSON config, returning the config
/// and any REMOVED-key deprecation warning it earned (issue #1696).
///
/// A preset is CQLite's own current shape and can never name a removed key, so it
/// carries no warning by construction.
fn config_from_string(s: &str) -> PyResult<(cqlite_core::Config, Option<String>)> {
    // Check for preset names
    match s {
        "memory_optimized" => Ok((cqlite_core::Config::memory_optimized(), None)),
        "performance_optimized" => Ok((cqlite_core::Config::performance_optimized(), None)),
        // Try parsing as JSON
        _ => cqlite_core::Config::from_json_str_reporting_removed(s, "JSON config string").map_err(
            |e| PyValueError::new_err(format!("Invalid config: not a preset name and {e}")),
        ),
    }
}

/// Convert a Python dict to Config via JSON bridge, returning the config and any
/// REMOVED-key deprecation warning it earned (issue #1696).
fn config_from_dict(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
) -> PyResult<(cqlite_core::Config, Option<String>)> {
    // Use Python's json module to serialize the dict
    let json_module = py.import("json")?;
    let json_str: String = json_module.call_method1("dumps", (dict,))?.extract()?;

    cqlite_core::Config::from_json_str_reporting_removed(&json_str, "config dict")
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Raise a Python `UserWarning` naming every REMOVED key the caller's config
/// still sets (issue #1696, roborev F1).
///
/// # Why the bindings need this
///
/// `cqlite_core::Config` is a Rust struct, so an embedder writing Rust who still
/// sets a deleted field gets a compile error — the loudest signal available.
/// Through this bridge there is no compile step: serde DISCARDS unknown fields, so
/// a pre-change config naming `performance`, `storage.block_size` or
/// `query.parallel` loaded SUCCESSFULLY and was silently ignored, the user
/// believing they had configured something. #1696 requires a LOUD signal at the
/// layer where a knob is set, and this is that layer for every non-Rust caller.
///
/// The posture matches the CLI's config file, deliberately and crate-wide:
/// **parse-and-ignore PLUS a named warning**, never `deny_unknown_fields`, which
/// would hard-fail an existing caller with no migration path over keys that never
/// did anything.
///
/// SCOPE, stated so this is not read as universal coverage: this makes the rule
/// true for callers who come through THESE entry points, which is every Python
/// caller. A Rust embedder who deserializes a `cqlite_core::Config` document with
/// plain serde bypasses the reporting constructor and still gets silence — issue
/// #3520 (#1696 roborev r2 F3).
///
/// A Python warning — not a `tracing` log (nothing subscribes in a Python
/// process) and not stderr — so it obeys the caller's own `warnings` filters and
/// is assertable from a test.
///
/// # Why `UserWarning` and not `DeprecationWarning` (#1696 roborev r2 F1)
///
/// Because Python HIDES `DeprecationWarning` under its default filters: the
/// stdlib installs `ignore::DeprecationWarning` with a single `default::…:__main__`
/// exception, so an ordinary user importing `cqlite` from any module other than
/// `__main__` saw NOTHING — the "loud signal at the layer where the knob is set"
/// was silent at exactly the layer this fix exists for. `UserWarning` matches no
/// `ignore` entry in the default list, so it is displayed without `-W` or a
/// `PYTHONWARNINGS` setting.
///
/// `UserWarning` over `FutureWarning` deliberately: `FutureWarning` means
/// "behaviour WILL change", while these keys are ALREADY removed and already
/// ignored. Nothing about them is pending.
///
/// The visibility itself is pinned by
/// `bindings/python/tests/test_config.py::test_removed_key_warning_is_visible_under_default_filters`,
/// which runs a subprocess under Python's own default filters — `pytest.warns`
/// enables ALL warnings, so it would pass for a hidden category too.
///
/// Called only once the operation has SUCCEEDED: the warning asserts the
/// configuration still loads, so it must not be raised before that is true
/// (#1696 roborev F3, and again on this surface at roborev r2 F2).
fn raise_removed_key_warning(py: Python<'_>, warning: Option<String>) -> PyResult<()> {
    if let Some(warning) = warning {
        let warnings = py.import("warnings")?;
        let category = py.get_type::<pyo3::exceptions::PyUserWarning>();
        warnings.call_method1("warn", (warning, category))?;
    }
    Ok(())
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
        let (config, _) = config_from_string("memory_optimized").unwrap();
        assert_eq!(config.memory.max_memory, 256 * 1024 * 1024);
    }

    #[test]
    fn test_config_from_preset_performance_optimized() {
        let (config, _) = config_from_string("performance_optimized").unwrap();
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
        let (config, _) = config_from_string(json).unwrap();
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
