//! Issue #1692 (AG3) — the schema-registry fallback tier of the sync
//! `get_table_schema` must never park a tokio worker thread.
//!
//! The offending code did `futures::executor::block_on(registry.read())` (and a
//! second `block_on` on the async `get_schema`) from a SYNC fn running on a tokio
//! worker. With a small runtime and a pending registry WRITE guard, the parked
//! workers can stall the whole runtime.
//!
//! This test reproduces that stall: a 2-worker runtime + a deliberately-held
//! registry WRITE guard + N concurrent resolutions that hit the fallback path.
//! On the pre-fix code every worker parks in `block_on` and the runtime deadlocks
//! (observed from a thread OUTSIDE the runtime so the timeout fires cleanly rather
//! than hanging the whole test binary). After the fix the schema is pre-resolved
//! into a sync cache at wiring time, so the sync path never touches the async
//! registry and all resolutions complete.

#![cfg(feature = "state_machine")]

use crate::schema::{SchemaRegistry, SchemaRegistryConfig, SchemaSource, TableSchema};
use crate::storage::sstable::reader::SSTableReader;
use crate::{Config, Platform};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

fn datasets_root() -> Option<PathBuf> {
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        let p = PathBuf::from(root);
        if p.is_dir() {
            return Some(p);
        }
    }
    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|p| p.join("test-data/datasets"))?;
    fallback.is_dir().then_some(fallback)
}

/// A real Cassandra 5.0 `nb` fixture. We use it only to obtain a genuine
/// `SSTableReader`; the header schema is cleared below so the registry-fallback
/// tier (Strategy 2) is exercised.
fn simple_table_data_db() -> Option<PathBuf> {
    let base = datasets_root()?.join("sstables/test_basic");
    let rd = std::fs::read_dir(&base).ok()?;
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_str()?.to_string();
        if name.starts_with("simple_table-") {
            let candidate = entry.path().join("nb-1-big-Data.db");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn simple_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_basic".to_string(),
        table: "simple_table".to_string(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        columns: Vec::new(),
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// N concurrent schema-fallback resolutions must complete on a 2-worker runtime
/// even while a registry WRITE guard is held. Pre-fix this deadlocks.
#[test]
fn concurrent_schema_fallback_does_not_stall_runtime() {
    let Some(path) = simple_table_data_db() else {
        eprintln!("SKIP: test_basic.simple_table fixture absent.");
        return;
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build 2-worker runtime");

    // Setup (async, no write guard held yet): open the reader, clear the header
    // schema so Strategy 1 misses, register the table schema, and wire+resolve
    // the registry.
    let (reader, registry) = rt.block_on(async {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
        let mut reader = SSTableReader::open(&path, &config, platform.clone())
            .await
            .expect("open nb fixture");

        // Force the header-schema tier (Strategy 1) to miss so the registry
        // fallback tier (Strategy 2) is the one that resolves.
        reader.schema = None;

        let registry_instance =
            SchemaRegistry::new(SchemaRegistryConfig::default(), platform, config)
                .await
                .expect("registry init");
        registry_instance
            .register_schema(simple_table_schema(), SchemaSource::Manual)
            .await
            .expect("register schema");
        let registry = Arc::new(tokio::sync::RwLock::new(registry_instance));

        reader.set_schema_registry(registry.clone());
        // Pre-resolve the registry schema into the sync cache while no write guard
        // is held (this is the #1692 fix — the sync path reads this cache instead
        // of block_on-ing the async registry).
        reader.resolve_registry_schema().await;

        (Arc::new(reader), registry)
    });

    // Hold a registry WRITE guard for the whole concurrent window. Pre-fix, every
    // `block_on(registry.read())` on a worker thread would park forever here.
    let write_guard = rt.block_on(async { registry.clone().write_owned().await });

    let n = 8usize;
    let (tx, rx) = mpsc::channel::<bool>();
    for _ in 0..n {
        let r = Arc::clone(&reader);
        let tx = tx.clone();
        rt.spawn(async move {
            let resolved = r.effective_schema().is_some();
            let _ = tx.send(resolved);
        });
    }
    drop(tx);

    // Observe from OUTSIDE the runtime so a stalled runtime times out cleanly.
    for i in 0..n {
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(resolved) => assert!(
                resolved,
                "schema-fallback resolution {i} returned None (expected a resolved schema)"
            ),
            Err(_) => panic!(
                "schema-fallback task {i} did not complete within timeout — the runtime \
                 stalled (#1692 regression: block_on on a worker thread while a registry \
                 write guard is held)"
            ),
        }
    }

    drop(write_guard);
}
