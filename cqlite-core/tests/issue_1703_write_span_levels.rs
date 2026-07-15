//! Issue #1703 (epic #1686, AI3 "observability honesty") + #2172(b): the
//! write-side `#[tracing::instrument]` spans MUST emit at DEBUG, not INFO, so
//! that at the CLI's default INFO level a batch of N mutations — through ANY of
//! the public write entry points — does NOT emit O(N) spans.
//!
//! # What this pins
//!
//! Every public write entry point drives a family of write-side spans:
//!   - sync `write()` / async `write_async()` → `write.mutation` (+ `wal.append`,
//!     `wal.sync`, `memtable.insert` per mutation);
//!   - sync `execute("INSERT …")` → `write.cql_execute`;
//!   - async `execute_flushing("INSERT …")` (the Node/Python binding path) →
//!     `write.cql_execute_flushing`, and when it crosses the flush threshold the
//!     flush spans `flush.memtable`;
//!   - async `flush()` → `flush.public` (+ nested `flush.memtable`);
//!   - `maintenance_step()` → `compaction.maintenance_step`,
//!     `compaction.scan_candidates`, `compaction.policy_select`.
//!
//! Before the #1703 demotion these defaulted to INFO, so a real subscriber at
//! INFO — the exact posture a CLI user / embedder experiences — saw O(N) spans
//! per batch. After the uniform DEBUG demotion, NONE of them reach a subscriber
//! capped at INFO, so the INFO-level span count is O(1) on every path.
//!
//! # Wiring evidence
//!
//! The count is observed through a REAL `tracing_subscriber::registry()`
//! subscriber with an `INFO` level filter (not a helper or a mock), proving the
//! *default output* is quiet.
//!
//! Installed as the **process-global** default (`set_global_default`), not a
//! thread-local (`with_default`). `execute_flushing`/`flush` cross `spawn_blocking`
//! onto separate tokio blocking-pool threads during the SSTable flush, and the
//! async phases run on a MULTI-thread runtime so those closures really do run off
//! the test task; a thread-local default would miss any INFO emitted there
//! (a vacuous-pass risk). `set_global_default` is observed by every thread. This
//! file contains exactly ONE `#[test]`, so calling it once is safe; the phases
//! run sequentially and RESET the captured span-name list between them.
//!
//! Routing: design-driven (OpenSpec change `demote-write-spans`). No production
//! behavior changes beyond the span level; names/keys are unchanged.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Durability, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;
use tracing::span::Attributes;
use tracing::{Event, Id, Subscriber};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

const KEYSPACE: &str = "ks_1703";
const TABLE: &str = "span_levels_t";
/// Number of mutations written per phase. Kept small so the per-write
/// `wal.sync` fsync stays cheap and deterministic.
const N: usize = 16;

/// Every write-side span name that #1703 demoted to DEBUG. NONE of these may be
/// observed through an INFO-capped subscriber on any public write path.
const DEMOTED_WRITE_SPANS: &[&str] = &[
    "write.mutation",
    "write.cql_execute",
    "write.cql_execute_flushing",
    "wal.append",
    "wal.sync",
    "memtable.insert",
    "flush.public",
    "flush.memtable",
    "compaction.policy_select",
    "compaction.maintenance_step",
    "compaction.scan_candidates",
];

/// Shared, thread-safe tallies for the counting subscriber below.
#[derive(Clone, Default)]
struct Tally {
    /// Names of every span created at or above the subscriber's level filter.
    span_names: Arc<Mutex<Vec<String>>>,
    /// Count of events (log lines) at or above the level filter.
    events: Arc<AtomicUsize>,
}

impl Tally {
    /// Clear the captured span names + event count between phases so each phase
    /// asserts against only the spans IT produced.
    fn reset(&self) {
        if let Ok(mut names) = self.span_names.lock() {
            names.clear();
        }
        self.events.store(0, Ordering::Relaxed);
    }

    /// Snapshot the captured INFO-level span names.
    fn info_spans(&self) -> Vec<String> {
        self.span_names.lock().expect("span names lock").clone()
    }
}

/// A `tracing` layer that records the NAME of every span it is handed and counts
/// events. Combined with an `INFO` `LevelFilter` it observes exactly what a
/// subscriber capped at INFO would see — DEBUG spans never reach it.
struct CountingLayer {
    tally: Tally,
}

impl<S> Layer<S> for CountingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        if let Ok(mut names) = self.tally.span_names.lock() {
            names.push(attrs.metadata().name().to_string());
        }
    }

    fn on_event(&self, _event: &Event<'_>, _ctx: Context<'_, S>) {
        self.tally.events.fetch_add(1, Ordering::Relaxed);
    }
}

fn span_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "value".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn mutation(i: usize) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("id", Value::Integer(i as i32)),
        None,
        vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text(format!("row-{i}")),
        }],
        1_000 + i as i64,
        None,
    )
}

fn insert_sql(i: usize) -> String {
    format!("INSERT INTO {KEYSPACE}.{TABLE} (id, value) VALUES ({i}, 'row-{i}')")
}

/// Build a fresh engine over a throwaway temp dir. `SyncEachWrite` so
/// `wal.append` + `wal.sync` fire per write; the caller picks the flush
/// threshold (`usize::MAX` to suppress auto-flush, or a small value to force a
/// mid-batch flush). The returned `TempDir` must outlive the engine.
fn build_engine(flush_threshold: usize) -> (TempDir, WriteEngine) {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        span_schema(),
    )
    .with_flush_threshold(flush_threshold)
    .with_durability(Durability::SyncEachWrite);
    let engine = WriteEngine::new(config).expect("engine");
    (temp_dir, engine)
}

/// Assert that a phase produced an O(1) number of INFO spans and that NONE of the
/// demoted write-side span names were observed at INFO.
fn assert_quiet(phase: &str, tally: &Tally) {
    let info_spans = tally.info_spans();
    let n_info = info_spans.len();

    // O(1), not O(N): the INFO span count must not scale with the batch size.
    assert!(
        n_info < N,
        "phase `{phase}`: expected O(1) INFO-level spans for a batch of N={N}, got \
         {n_info}: {info_spans:?} — a write-side span is still at INFO (issue #1703)"
    );

    for name in DEMOTED_WRITE_SPANS {
        assert!(
            !info_spans.iter().any(|s| s == name),
            "phase `{phase}`: span `{name}` was emitted at INFO but MUST be DEBUG \
             (issue #1703); captured INFO spans: {info_spans:?}"
        );
    }
}

/// Every public write entry point, observed through a real INFO-capped
/// subscriber, must emit an O(1) number of INFO-level spans — NOT the O(N) the
/// write spans produced when they defaulted to INFO. The phases run under ONE
/// process-global subscriber (one `set_global_default` per binary), resetting
/// the captured span-name list between them.
///
/// RED before the demotion (each write/wal/memtable/flush/compaction span is
/// INFO, so O(N) names are captured); GREEN after (0 demoted names).
#[test]
fn write_paths_emit_no_info_spans() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("multi-thread runtime");

    let tally = Tally::default();
    let layer = CountingLayer {
        tally: tally.clone(),
    }
    .with_filter(LevelFilter::INFO);
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("set_global_default must succeed (only test in this binary)");

    // ---- Phase 1: sync write() batch (write.mutation + wal.* + memtable.insert)
    {
        let (_tmp, mut engine) = build_engine(usize::MAX);
        for i in 0..N {
            engine.write(mutation(i)).expect("sync write");
        }
    }
    assert_quiet("sync write()", &tally);
    tally.reset();

    // ---- Phase 2: sync execute("INSERT …") (write.cql_execute)
    {
        let (_tmp, mut engine) = build_engine(usize::MAX);
        for i in 0..N {
            engine.execute(&insert_sql(i)).expect("sync execute");
        }
    }
    assert_quiet("sync execute()", &tally);
    tally.reset();

    // ---- Phase 3: async write_async().await (write.mutation)
    rt.block_on(async {
        let (_tmp, mut engine) = build_engine(usize::MAX);
        for i in 0..N {
            engine.write_async(mutation(i)).await.expect("write_async");
        }
    });
    assert_quiet("async write_async()", &tally);
    tally.reset();

    // ---- Phase 4: async execute_flushing().await crossing the flush threshold
    // (write.cql_execute_flushing + flush.memtable, which crosses spawn_blocking).
    rt.block_on(async {
        // A tiny flush threshold so the memtable crosses it mid-batch and the
        // flush path (spanning spawn_blocking onto worker threads) really runs.
        let (_tmp, mut engine) = build_engine(64);
        for i in 0..N {
            engine
                .execute_flushing(&insert_sql(i))
                .await
                .expect("execute_flushing");
        }
    });
    assert_quiet("async execute_flushing() with flush", &tally);
    tally.reset();

    // ---- Phase 5: explicit async flush() (flush.public + nested flush.memtable)
    rt.block_on(async {
        let (_tmp, mut engine) = build_engine(usize::MAX);
        for i in 0..N {
            engine.write_async(mutation(i)).await.expect("write_async");
        }
        engine.flush().await.expect("flush");
    });
    assert_quiet("async flush()", &tally);
    tally.reset();

    // ---- Phase 6: maintenance_step() (compaction.maintenance_step /
    // scan_candidates / policy_select). A single step exercises all three
    // compaction spans regardless of whether a merge is actually selected — the
    // policy scan + select run on every step — so no heavy 4-SSTable setup is
    // needed to pin the level.
    {
        let (_tmp, mut engine) = build_engine(64);
        for i in 0..N {
            engine
                .execute(&insert_sql(i))
                .expect("execute for compaction setup");
        }
        rt.block_on(async {
            engine.flush().await.expect("flush before maintenance");
        });
        tally.reset();
        engine
            .maintenance_step(Duration::from_millis(50))
            .expect("maintenance_step");
    }
    assert_quiet("maintenance_step()", &tally);
}
