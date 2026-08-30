//! The `Database.open()` factory: option validation, core open (with or
//! without a schema) and optional write-engine construction.
//!
//! Split out of `database.rs` under the campsite rule (epic #1116, issue
//! #1464). Pure code motion; the only change is that the inlined single-table
//! schema parser is now called as `write::parse_single_table_schema`.

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[cfg(feature = "write-support")]
use std::sync::Mutex;

use napi_derive::napi;

use crate::error::{runtime_init_error, to_napi_error};

use super::{Database, DatabaseOptions};

#[napi]
impl Database {
    /// Opens a database at the specified data directory.
    ///
    /// @param dataDir - Path to the SSTable data directory
    /// @param options - Optional configuration (schema path, writable, writeDir, etc.)
    /// @returns Promise resolving to a Database instance
    ///
    /// @example
    /// ```javascript
    /// // Basic open (read-only)
    /// const db = await Database.open('/path/to/sstables');
    ///
    /// // With schema file
    /// const db = await Database.open('/path/to/sstables', {
    ///   schema: '/path/to/schema.cql'
    /// });
    ///
    /// // With write support enabled
    /// const db = await Database.open('/path/to/sstables', {
    ///   schema: '/path/to/schema.cql',
    ///   writable: true,
    ///   writeDir: '/tmp/cqlite-writes',
    /// });
    /// ```
    #[napi(factory)]
    pub async fn open(
        data_dir: String,
        options: Option<DatabaseOptions>,
    ) -> napi::Result<Database> {
        let path = PathBuf::from(&data_dir);

        // Initialise observability ONCE per process from the open options (or the
        // CQLITE_OTEL_* env fallback). Later opens are no-ops; the first wins.
        // This must run before any span is created below so the OTel layer is
        // composed in. Safe + inert when telemetry is disabled or the feature is
        // off (issue #1040).
        crate::observability::init_once(options.as_ref().and_then(|o| o.otel.as_ref()));

        // Eagerly build the shared tokio runtime so a resource-starved host
        // (out of threads/FDs/memory) surfaces a catchable error AT open()
        // rather than only later on the first executeNative/streaming next/flush
        // block_on (issue #1438). On success the runtime is memoized and reused;
        // this call is a cheap OnceLock fast-path on every subsequent open.
        crate::runtime::try_get_runtime().map_err(runtime_init_error)?;

        // Per-handle default traceparent for this database's per-call spans.
        let traceparent = options
            .as_ref()
            .and_then(|o| o.traceparent.clone())
            .filter(|t| !t.trim().is_empty());

        // Extract all options and build config
        let (schema_path, core_config, writable, write_dir) = if let Some(opts) = options {
            let mut config = cqlite_core::Config::default();

            if let Some(limit) = opts.memory_limit {
                if !limit.is_finite() {
                    return Err(napi::Error::from_reason(
                        "memoryLimit must be a finite number",
                    ));
                }
                if limit < 1.0 {
                    return Err(napi::Error::from_reason(
                        "memoryLimit must be at least 1 byte",
                    ));
                }
                config.memory.max_memory = limit as u64;
            }

            if let Some(enabled) = opts.cache_enabled {
                // Issue #1568: the decorative row_cache/query_cache config knobs
                // were deleted; the block/chunk cache is the single real cache.
                config.memory.block_cache.enabled = enabled;
            }

            if let Some(ac) = opts.auto_compaction {
                config.storage.compaction.auto_compaction = ac;
            }

            let writable = opts.writable.unwrap_or(false);
            let write_dir = opts.write_dir.map(PathBuf::from);

            // Validate the optional flush threshold (issue #1620). Bytes as an
            // f64 (napi-idiomatic; matches `memoryLimit`). Must be finite, at
            // least 1 byte, and no greater than the memtable hard limit.
            if let Some(v) = opts.flush_threshold {
                if !v.is_finite() {
                    return Err(napi::Error::from_reason(
                        "flushThreshold must be a finite number",
                    ));
                }
                if v < 1.0 {
                    return Err(napi::Error::from_reason(
                        "flushThreshold must be at least 1 byte",
                    ));
                }
                // A threshold at or above the hard limit would never trigger an
                // auto-flush: the memtable hits the ceiling and rejects writes
                // first, dead-ending the binding write path (roborev 2885/2890,
                // issue #1620). Read from the CALLER's public knob, so raising
                // the ceiling raises what is accepted. NOT gated on
                // `write-support`: `memtable_hard_limit` is feature-independent,
                // so a gate would only make the same `flushThreshold` accepted in
                // one build and rejected in another, with nothing behind it.
                // `>=` matches `Config::validate`'s STRICT headroom rule (#1697
                // r3); Node never calls `validate`, so it alone could wedge.
                let hard_limit = config.storage.memtable_hard_limit;
                if v >= hard_limit as f64 {
                    return Err(napi::Error::from_reason(format!(
                        "flushThreshold ({v} bytes) must be less than the memtable hard limit \
                         ({hard_limit} bytes); equal leaves no headroom to flush into"
                    )));
                }
                // Issue #1697: applied to the PUBLIC knob rather than the
                // engine's private `with_flush_threshold` setter, so the option
                // reaches the engine through the ONE bridge below. External
                // behaviour is unchanged; only the route is.
                config.storage.memtable_size_threshold = v as u64;
            }

            (opts.schema.map(PathBuf::from), config, writable, write_dir)
        } else {
            (None, cqlite_core::Config::default(), false, None)
        };

        // Clone schema path before it is potentially consumed by db open, so the
        // write engine initializer can also read the same CQL file.
        #[cfg(feature = "write-support")]
        let schema_path_for_write: Option<PathBuf> = schema_path.clone();

        // Capture the whole public config before `core_config` is moved into
        // ingestion / Database::open, so `Config.storage` is authoritative for
        // the write path (issues #1619, #1697) rather than decorative; the ONE
        // bridge `WriteEngineConfig::from_config` translates it below.
        #[cfg(feature = "write-support")]
        let write_engine_public_config = core_config.clone();

        // Validate write options
        #[cfg(feature = "write-support")]
        if writable && write_dir.is_none() {
            return Err(napi::Error::from_reason(
                "writeDir is required when writable is true",
            ));
        }

        let db = if let Some(schema) = schema_path {
            // Use ingestion module for schema + SSTable discovery
            let ingestion_config = cqlite_core::ingestion::IngestionConfig {
                schema_paths: vec![schema],
                data_dir: path,
                version_hint: None,
                core_config,
                table_directory_filter: None,
            };

            let result = cqlite_core::ingestion::ingest(ingestion_config)
                .await
                .map_err(to_napi_error)?;

            result.database
        } else {
            // Simple open without schema
            cqlite_core::Database::open(&path, core_config)
                .await
                .map_err(to_napi_error)?
        };

        // Build write engine if requested
        #[cfg(feature = "write-support")]
        let write_engine_opt: Option<
            Arc<Mutex<cqlite_core::storage::write_engine::WriteEngine>>,
        > = if writable {
            let wd = write_dir
                .as_ref()
                .expect("write_dir validated above to be Some when writable");

            // We need a TableSchema for the write engine.
            // Use the ingestion result schema registry if available, otherwise require
            // schema_path to be provided and parse it directly (mirrors CLI write-only mode).
            let schema = if let Some(ref sp) = schema_path_for_write {
                super::write::parse_single_table_schema(sp)?
            } else {
                return Err(napi::Error::from_reason(
                    "A schema file (option `schema`) is required when `writable` is true.",
                ));
            };

            // Via the single bridge (#1697); `flushThreshold` was folded into
            // `storage.memtable_size_threshold` above, else the 64 MB default.
            let config = cqlite_core::storage::write_engine::WriteEngineConfig::from_config(
                &write_engine_public_config,
                wd.join("data"),
                wd.join("wal"),
                schema,
            );

            let engine = cqlite_core::storage::write_engine::WriteEngine::new(config)
                .map_err(to_napi_error)?;
            Some(Arc::new(Mutex::new(engine)))
        } else {
            None
        };

        #[cfg(not(feature = "write-support"))]
        let _ = (writable, write_dir); // suppress unused warning when feature off

        Ok(Database {
            inner: Arc::new(db),
            closed: AtomicBool::new(false),
            traceparent,
            #[cfg(feature = "write-support")]
            write_engine: write_engine_opt,
        })
    }
}
