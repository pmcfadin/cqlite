//! Issue #1703 (epic #1686, AI3 "observability honesty"): the write-side
//! `#[tracing::instrument]` spans MUST emit at DEBUG, not INFO, so that at the
//! CLI's default INFO level a batch of N mutations does NOT emit O(N) spans.
//!
//! # What this pins
//!
//! A batch of N mutations drives, per mutation, the `write.mutation`,
//! `wal.append`, `wal.sync`, and `memtable.insert` spans. Before this change all
//! four defaulted to INFO (no `level` on the attribute), so a real subscriber
//! installed at INFO — the exact posture a CLI user / embedder experiences — saw
//! ~3–4N spans per batch. After the uniform DEBUG demotion, none of those spans
//! reach a subscriber capped at INFO, so the INFO-level span count is O(1).
//!
//! # Wiring evidence
//!
//! The count is observed through a REAL `tracing_subscriber::registry()`
//! subscriber with an `INFO` level filter (not a helper or a mock) — proving the
//! *default output* is quiet, exactly what a CLI user sees.
//!
//! Routing: design-driven (OpenSpec change `demote-write-spans`). No production
//! behavior changes beyond the span level; names/keys are unchanged.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
/// Number of mutations written in the batch. Kept small so the per-write
/// `wal.sync` fsync stays cheap and deterministic.
const N: usize = 16;

/// Shared, thread-safe tallies for the counting subscriber below.
#[derive(Clone, Default)]
struct Tally {
    /// Names of every span created at or above the subscriber's level filter.
    span_names: Arc<Mutex<Vec<String>>>,
    /// Count of events (log lines) at or above the level filter.
    events: Arc<AtomicUsize>,
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

/// A batch of N mutations, observed through a real INFO-capped subscriber, must
/// emit an O(1) number of INFO-level spans — NOT the ~3–4N the write spans
/// produced when they defaulted to INFO. Specifically, none of the write-side
/// span names may appear at INFO.
///
/// RED before the demotion (each `write.mutation` / `wal.append` / `wal.sync` /
/// `memtable.insert` span is INFO, so ~4N names are captured); GREEN after (0).
#[test]
fn write_batch_emits_no_info_spans() {
    let tally = Tally::default();
    let layer = CountingLayer {
        tally: tally.clone(),
    }
    .with_filter(LevelFilter::INFO);
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        let temp_dir = TempDir::new().expect("tempdir");
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            span_schema(),
        )
        // High threshold so the batch never auto-flushes: we are measuring the
        // per-mutation write spans, not flush spans.
        .with_flush_threshold(usize::MAX)
        // SyncEachWrite so `wal.append` + `wal.sync` fire per write — the exact
        // per-mutation spans the audit flagged.
        .with_durability(Durability::SyncEachWrite);

        let mut engine = WriteEngine::new(config).expect("engine");
        for i in 0..N {
            engine.write(mutation(i)).expect("write");
        }
    });

    let info_spans = tally.span_names.lock().expect("span names lock").clone();
    let n_info = info_spans.len();

    // O(1), not O(N): the count must not scale with the batch size.
    assert!(
        n_info < N,
        "expected O(1) INFO-level spans for a batch of N={N} writes, got {n_info}: {info_spans:?} \
         — the write-side spans are still at INFO (issue #1703 demotion missing)"
    );

    // None of the demoted write-side spans may be observed at INFO.
    for name in ["write.mutation", "wal.append", "wal.sync", "memtable.insert"] {
        assert!(
            !info_spans.iter().any(|s| s == name),
            "span `{name}` was emitted at INFO but MUST be DEBUG (issue #1703); \
             captured INFO spans: {info_spans:?}"
        );
    }
}
