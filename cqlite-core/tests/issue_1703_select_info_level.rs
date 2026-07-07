//! Issue #1703 (epic #1686, AI3 "observability honesty"): a single SELECT
//! executed through the public query API MUST emit AT MOST ONE INFO-level line
//! at the default level — the per-query `tracing::info!` chatter in the SELECT
//! executor is demoted to DEBUG.
//!
//! # What this pins
//!
//! Before this change the SELECT path logged ~5–7 `info!` lines per query
//! ("Found schema for …", "Scan returned N rows", the point-lookup lines, the
//! materialization line, …), all at the default INFO level. After the demotion
//! those become DEBUG, so a subscriber capped at INFO sees ≤1 line per SELECT.
//!
//! # Wiring evidence
//!
//! The count is observed through a REAL `tracing_subscriber::registry()`
//! subscriber with an `INFO` `LevelFilter` — the exact posture a CLI user /
//! embedder runs in — not a helper or mock.
//!
//! Requires `CQLITE_DATASETS_ROOT` + fetched binaries; skips (never fails) when
//! the fixture is absent, and treats a present-but-0-rows scan as a hard failure
//! (a real scan must run for the info lines to fire).
//!
//! Routing: design-driven (OpenSpec change `demote-write-spans`). Message
//! content is unchanged (AG5/#1694 owns content); only the level changes.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;
use tracing::span::Attributes;
use tracing::{Event, Id, Subscriber};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

/// Counts INFO-level events (log lines) seen through the subscriber.
#[derive(Clone, Default)]
struct EventTally {
    events: Arc<AtomicUsize>,
}

struct CountingLayer {
    tally: EventTally,
}

impl<S> Layer<S> for CountingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {}

    fn on_event(&self, _event: &Event<'_>, _ctx: Context<'_, S>) {
        self.tally.events.fetch_add(1, Ordering::Relaxed);
    }
}

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        if let Some(dir) = root.parent().and_then(|p| {
            let d = p.join("schemas");
            d.exists().then_some(d)
        }) {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

fn fixture_data_present(keyspace: &str, table: &str) -> bool {
    let Some(root) = datasets_root() else {
        return false;
    };
    let Ok(entries) = std::fs::read_dir(root.join("sstables").join(keyspace)) else {
        return false;
    };
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        if let Ok(files) = std::fs::read_dir(e.path()) {
            for f in files.flatten() {
                if f.file_name().to_string_lossy().ends_with("-Data.db") {
                    return true;
                }
            }
        }
    }
    false
}

async fn setup(keyspace: &str, schema_file: &str) -> Option<Database> {
    let root = datasets_root()?;
    let schema_path = schemas_dir()?.join(schema_file);
    if !schema_path.exists() {
        return None;
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return None;
    }
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        return None;
    }
    Some(result.database)
}

/// One full-scan SELECT, observed through a real INFO-capped subscriber, must
/// emit AT MOST ONE INFO line. RED before the demotion (~2+ info lines: "Found
/// schema …" + "Scan returned N rows"); GREEN after (0).
#[tokio::test]
async fn select_emits_at_most_one_info_line() {
    if !fixture_data_present("test_basic", "simple_table") {
        eprintln!("Skipping (#1703): test_basic/simple_table Data.db not present");
        return;
    }
    let Some(db) = setup("test_basic", "basic-types.cql").await else {
        eprintln!("Skipping (#1703): could not ingest test_basic");
        return;
    };

    let tally = EventTally::default();
    let layer = CountingLayer {
        tally: tally.clone(),
    }
    .with_filter(LevelFilter::INFO);
    let subscriber = tracing_subscriber::registry().with(layer);

    // Drive the SELECT with the subscriber active across the `.await`. This test
    // runs on the current-thread tokio runtime (`#[tokio::test]` default), so the
    // task never migrates threads and the thread-local default subscriber applies
    // for the whole query. The guard keeps it installed until dropped.
    let _guard = tracing::subscriber::set_default(subscriber);
    let result = db
        .execute("SELECT * FROM test_basic.simple_table")
        .await
        .expect("scan");
    drop(_guard);

    // A present fixture must return rows — otherwise the scan info sites never
    // fired and the assertion below would pass vacuously.
    assert!(
        result.rows.len() > 100,
        "present fixture must return its full row set (got {}) — 0/low rows = read \
         regression, not a skip",
        result.rows.len()
    );

    let info_lines = tally.events.load(Ordering::Relaxed);
    assert!(
        info_lines <= 1,
        "a single SELECT emitted {info_lines} INFO lines, expected ≤1 — the per-query \
         SELECT `info!` chatter must be demoted to DEBUG (issue #1703)"
    );
}
