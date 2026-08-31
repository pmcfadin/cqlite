//! The `cqlite.open()` pyfunction: config resolution, schema ingestion and
//! optional write-engine construction.
//!
//! Split out of `database.rs` under the campsite rule (epic #1116, issue
//! #1464). Pure code motion; the signature, pyo3 attributes and doc comments
//! are unchanged from the single-file layout.

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::config::config_for_open;
use crate::error::{runtime_init_to_py_err, to_py_err};
use crate::runtime::block_on;
use crate::write::PyWriteEngine;

use tokio::sync::Mutex as AsyncMutex;

use super::Database;

/// Open a CQLite database.
///
/// Opens a database at the specified path, optionally loading a schema file
/// and applying custom configuration.
///
/// # Arguments
///
/// * `path`       – Path to the data directory containing SSTables
/// * `schema`     – Optional path to a CQL schema file (.cql)
/// * `config`     – Optional configuration (dict, JSON string, or preset name)
/// * `writable`   – Enable write support (INSERT / UPDATE / DELETE).  Default `False`.
/// * `write_dir`  – Directory for WAL and flushed SSTables.  Required when
///                  `writable=True`.  Created automatically if it doesn't exist.
///
/// # Caveat: write_dir is not file-locked
///
/// Only one `Database` instance should hold a given `write_dir` at a time.
/// Concurrent instances sharing the same directory are **not** protected by
/// file locks and will corrupt each other's WAL and SSTable files.  File
/// locking is planned for a future release (see issue #485).
///
/// # Returns
///
/// A new Database instance.
///
/// # Raises
///
/// * `IOError`     – If the path doesn't exist or is inaccessible
/// * `SchemaError` – If schema parsing fails
/// * `ValueError`  – If configuration is invalid, or `writable=True` but
///                   `write_dir` or `schema` was not provided
///
/// # Examples
///
/// ```python
/// # Basic read-only open
/// db = cqlite.open("/path/to/sstables")
///
/// # With schema file
/// db = cqlite.open("/path/to/sstables", schema="/path/to/schema.cql")
///
/// # With write support
/// db = cqlite.open(
///     "/path/to/sstables",
///     schema="/path/to/schema.cql",
///     writable=True,
///     write_dir="/tmp/cqlite-write",
/// )
///
/// # Using context manager
/// with cqlite.open("/path/to/sstables") as db:
///     pass
/// ```
#[pyfunction]
#[pyo3(signature = (path, *, schema=None, config=None, writable=false, write_dir=None, flush_threshold=None, otel_config=None, traceparent=None))]
pub fn open(
    py: Python<'_>,
    path: PathBuf,
    schema: Option<PathBuf>,
    config: Option<&Bound<'_, PyAny>>,
    writable: bool,
    write_dir: Option<PathBuf>,
    flush_threshold: Option<u64>,
    otel_config: Option<&Bound<'_, PyAny>>,
    traceparent: Option<String>,
) -> PyResult<Database> {
    // Initialise observability once per process (issue #1039). The config is
    // resolved from the optional `otel_config` dict layered over the
    // `CQLITE_OTEL_*` environment. The first `open` wins; later opens reuse the
    // installed exporters. A bad exporter config never blocks the open — it
    // simply yields no telemetry.
    let obs_cfg = crate::observability::config_from_py(py, otel_config)?;
    crate::observability::ensure_initialized(obs_cfg);

    // Validate the config-INDEPENDENT half of the flush threshold upfront (#1620),
    // regardless of `writable`, matching Node: a `0` threshold would make
    // `should_flush(0)` true after every write. The CEILING half depends on the
    // caller's `memtable_hard_limit`, so it lives beside the fold below.
    if let Some(v) = flush_threshold {
        if v < 1 {
            return Err(PyValueError::new_err(
                "flush_threshold must be at least 1 byte",
            ));
        }
    }

    // Validate writable-mode requirements upfront before any I/O.
    if writable {
        if write_dir.is_none() {
            return Err(PyValueError::new_err(
                "write_dir is required when writable=True. \
                 Example: cqlite.open(path, writable=True, write_dir='/tmp/cqlite-writes')",
            ));
        }
        if schema.is_none() {
            return Err(PyValueError::new_err(
                "schema is required when writable=True so that the write engine \
                 can resolve column types.",
            ));
        }
    }

    // Assembled in one place (`config::config_for_open`): parse, fold
    // `flush_threshold`, validate the MERGED result. `Config.storage` is
    // therefore authoritative for the Python write path rather than decorative
    // (#1619, #1697); the ONE bridge `WriteEngineConfig::from_config` translates
    // it below.
    let core_config = config_for_open(py, config, flush_threshold)?;
    let write_engine_public_config = core_config.clone();

    // Open the read-side database and capture the schema registry when present.
    // We always use ingestion when a schema file is provided because that path
    // performs the SSTable discovery and populates the schema registry.
    let (db, schema_registry_opt) = if let Some(schema_path) = schema.clone() {
        let ingestion_config = cqlite_core::ingestion::IngestionConfig {
            schema_paths: vec![schema_path],
            data_dir: path.clone(),
            version_hint: None,
            core_config,
            table_directory_filter: None,
        };

        py.allow_threads(|| {
            block_on(async {
                let result = cqlite_core::ingestion::ingest(ingestion_config).await?;
                let registry = result.schema_registry;
                Ok::<_, cqlite_core::Error>((result.database, Some(registry)))
            })
        })
        .map_err(runtime_init_to_py_err)?
        .map_err(to_py_err)?
    } else {
        let db = py
            .allow_threads(|| block_on(cqlite_core::Database::open(&path, core_config)))
            .map_err(runtime_init_to_py_err)?
            .map_err(to_py_err)?;
        (db, None)
    };

    // Build WriteEngine when writable=True.
    let write_engine: Option<Arc<AsyncMutex<PyWriteEngine>>> = if writable {
        let wd = write_dir.expect("validated above");
        let schema_path_display = schema.clone().expect("validated above");

        // Retrieve the first TableSchema from the registry loaded during ingestion.
        let table_schema = py
            .allow_threads(|| {
                block_on(async {
                    let registry = schema_registry_opt.ok_or_else(|| {
                        cqlite_core::Error::Schema(
                            "Internal error: schema registry unavailable. \
                             This should not happen when schema= is provided."
                                .to_string(),
                        )
                    })?;

                    let schemas = registry
                        .read()
                        .await
                        .list_schemas(None)
                        .await
                        .map_err(|e| {
                            cqlite_core::Error::Schema(format!("Failed to list schemas: {}", e))
                        })?;

                    schemas.into_iter().next().ok_or_else(|| {
                        cqlite_core::Error::Schema(format!(
                            "No table schema found in {:?}. \
                             Verify the schema file contains at least one CREATE TABLE statement.",
                            schema_path_display
                        ))
                    })
                })
            })
            .map_err(runtime_init_to_py_err)?
            .map_err(to_py_err)?;

        // Via the single bridge (#1697).
        let engine_config = cqlite_core::storage::write_engine::WriteEngineConfig::from_config(
            &write_engine_public_config,
            wd.join("data"),
            wd.join("wal"),
            table_schema,
        );

        let engine = cqlite_core::storage::write_engine::WriteEngine::new(engine_config)
            .map_err(to_py_err)?;

        Some(Arc::new(AsyncMutex::new(PyWriteEngine::new(engine))))
    } else {
        // Silence unused-variable warning
        let _ = (
            write_dir,
            schema_registry_opt,
            write_engine_public_config,
            flush_threshold,
        );
        None
    };

    Ok(Database {
        inner: Arc::new(db),
        closed: Arc::new(AtomicBool::new(false)),
        write_engine,
        default_traceparent: traceparent.filter(|s| !s.trim().is_empty()),
    })
}
