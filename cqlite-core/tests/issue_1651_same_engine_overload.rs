//! Same-engine overload / backpressure integration test (Issue #1651, the "O3"
//! blind-spot guard).
//!
//! # What this pins
//!
//! This is the standalone guard for the L2 auto-flush *cliff* that the N2 fix
//! (issue #1620, MERGED via PR #1783) closed. It drives **sustained ingest +
//! flush + compaction on ONE shared `WriteEngine` and ONE table, from INSIDE a
//! Tokio runtime** — the exact shape the Node/Python bindings run in.
//!
//! Before N2, the sync `write()` auto-flush was gated on
//! `tokio::runtime::Handle::try_current().is_err()`, so binding writes (which
//! run inside a runtime) never auto-flushed: the memtable grew past the flush
//! threshold, hit the hard limit at the admission gate, and every subsequent
//! write dead-ended with "Memtable at hard limit". N2 routed the binding write
//! path through a real async flush (`WriteEngine::write_async` /
//! `execute_flushing`) that DOES flush from inside a runtime.
//!
//! This test asserts against the landed N2 fix, so **it is green iff N2 landed**.
//! It is deliberately NOT structured like the `read_while_write` bench, which
//! isolates one engine per writer and therefore cannot observe the shared-engine
//! backpressure cliff this test exists to catch.
//!
//! Routing: oracle/invariant-driven test-infra — no OpenSpec, no production
//! change (per issue #1651).

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Durability, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

const KEYSPACE: &str = "ks_1651";
const TABLE: &str = "overload_t";

/// Small, deterministic thresholds so the ingest loop reaches the flush trigger
/// quickly and many times without any wall-clock timing. Each mutation is
/// ~320 bytes (see `PAYLOAD_LEN`), so a flush fires roughly every ~13 writes and
/// the hard limit is ~200 writes away — i.e. the flush trigger is crossed far
/// below the hard limit on every cycle.
const FLUSH_THRESHOLD: usize = 4 * 1024; // 4 KiB
const HARD_LIMIT: usize = 64 * 1024; // 64 KiB
/// Deterministic per-write text payload length (bytes). Dominates the mutation
/// size estimate so the thresholds above behave predictably.
const PAYLOAD_LEN: usize = 256;
/// Sustained write count — enough for many flush + compaction cycles.
const WRITE_COUNT: i32 = 2_000;
/// Interleave a compaction maintenance step every N writes on the SAME engine.
const COMPACTION_EVERY: i32 = 25;

fn overload_schema() -> TableSchema {
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

/// Deterministic (seeded-by-index) mutation: partition id `i`, a fixed-length
/// payload whose bytes are derived from `i` so input is reproducible with no RNG.
fn deterministic_mutation(i: i32) -> Mutation {
    // Fixed-length payload derived from the index — deterministic, no wall clock,
    // no RNG. Length is constant so the size estimate is stable across writes.
    let fill = (b'a' + (i % 26) as u8) as char;
    let payload: String = fill.to_string().repeat(PAYLOAD_LEN);
    Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("id", Value::Integer(i)),
        None,
        vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text(payload),
        }],
        // Monotonic, deterministic write timestamp (not wall-clock).
        1_000 + i as i64,
        None,
    )
}

/// Sustained ingest + auto-flush + compaction on ONE shared engine + ONE table,
/// driven from inside a Tokio runtime via the N2 async write path
/// (`WriteEngine::write_async`), interleaved with `maintenance_step` compaction
/// on that same engine.
///
/// Asserts the two properties that regress the moment N2 is reverted:
///   1. **Bounded memory** — the memtable's `memtable_size()` never climbs
///      monotonically toward the 64 KiB hard limit; it stays in a tight sawtooth
///      near the flush threshold (proved by a hard ceiling far below the hard
///      limit AND by observing at least one flush-induced decrease).
///   2. **No error cliff** — every write in the sustained loop returns `Ok`;
///      none fails with "Memtable at hard limit".
#[tokio::test]
async fn same_engine_sustained_ingest_stays_bounded_and_never_cliffs() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        overload_schema(),
    )
    .with_flush_threshold(FLUSH_THRESHOLD)
    .with_hard_limit(HARD_LIMIT)
    // Durability is not the subject here; disabling per-write fsync keeps the
    // sustained loop fast and deterministic (no fsync-latency flakiness). The
    // auto-flush cliff this test guards lives on the memtable path, not the WAL.
    .with_durability(Durability::Disabled);

    let mut engine = WriteEngine::new(config).expect("engine");

    // Sample the memtable size after every write to characterize the sawtooth.
    let mut sizes: Vec<usize> = Vec::with_capacity(WRITE_COUNT as usize);

    for i in 0..WRITE_COUNT {
        // Binding shape: async write path introduced by N2. Inside a Tokio
        // runtime this auto-flushes when the threshold is crossed. Pre-N2 this
        // would eventually error with "Memtable at hard limit"; post-N2 every
        // write must succeed.
        engine
            .write_async(deterministic_mutation(i))
            .await
            .unwrap_or_else(|e| panic!("write {i} must return Ok post-N2 (#1620), got: {e}"));

        sizes.push(engine.memtable_size());

        // Interleave compaction on the SAME engine so ingest + flush + compaction
        // all run concurrently against one shared engine (single-writer model:
        // serialized within the runtime, exactly as the bindings drive it).
        if i % COMPACTION_EVERY == 0 {
            engine
                .maintenance_step(Duration::from_millis(20))
                .expect("maintenance_step must not error under sustained ingest");
        }
    }

    // ---- Assertion 1: bounded memory (no monotonic climb to the hard limit) ----
    let max_size = sizes.iter().copied().max().unwrap_or(0);
    assert!(
        max_size < HARD_LIMIT,
        "memtable size {max_size} reached/approached the hard limit {HARD_LIMIT} — the pre-N2 \
         auto-flush cliff has regressed (issue #1620)"
    );
    // Tight sawtooth: auto-flush fires as soon as the threshold is crossed, so
    // the peak stays within one flush cycle of the threshold, nowhere near the
    // hard limit. A generous 4x-threshold ceiling proves boundedness without
    // being brittle to the size estimate.
    assert!(
        max_size < FLUSH_THRESHOLD * 4,
        "memtable peak {max_size} exceeded 4x the flush threshold {FLUSH_THRESHOLD}; auto-flush \
         is not keeping the memtable bounded from inside the runtime (issue #1620)"
    );
    // Non-monotonic: at least one flush must have drained the memtable, i.e. the
    // sampled size must decrease somewhere. A monotonic climb is the pre-N2 cliff.
    let saw_flush_drop = sizes.windows(2).any(|w| w[1] < w[0]);
    assert!(
        saw_flush_drop,
        "memtable size never decreased across {WRITE_COUNT} writes — auto-flush never fired \
         inside the runtime (pre-N2 cliff, issue #1620)"
    );

    // ---- Assertion 2: no error cliff (implicit above) ----
    // Every `write_async` above returned Ok (or the loop panicked). Re-state the
    // count for clarity: all sustained writes were accepted.
    assert_eq!(
        sizes.len(),
        WRITE_COUNT as usize,
        "every one of {WRITE_COUNT} sustained writes must be accepted with no hard-limit cliff"
    );

    // Drain the tail so the engine shuts down cleanly.
    engine.flush().await.expect("final flush");
}
