//! Flight-producer allocation / peak-memory budget guard (issue #1494, AD5;
//! dhat-gated).
//!
//! The Flight `do_get` path funnels every streamed row through the
//! `MergeProducer` (k-way SSTable merge → CQL→Arrow conversion). This guard pins
//! today's total-allocated + peak-heap figures for a full `produce()` over a
//! flushed fixture as a load-deterministic regression net — the hard,
//! machine-independent signal the spec designates for the producer path (a
//! wall-clock throughput number flakes under load; dhat byte totals do not).
//! AB1/AB3/AB7 tighten these bounds; this change lands them PASSING as baseline
//! locks ("do not regress above today").
//!
//! **Reuses the epic-H machinery** (the `#[global_allocator] dhat::Alloc` +
//! `HeapStats` pattern of `cqlite-core/tests/memory_budget.rs`) rather than
//! duplicating it.
//!
//! **Non-vacuous by construction**: it asserts the producer emitted ≥ 1 row AND
//! that total allocated bytes are > 0 before checking the bound, so a run that
//! measured nothing FAILS rather than passing at "0 ≤ budget". The fixture is
//! self-contained (a WriteEngine flush), so there is no external-dataset skip.
//!
//! Run via (the `memory-budget` gate component runs this):
//! ```text
//! cargo test -p cqlite-flight --features dhat-heap \
//!   --test issue_1494_producer_mem_budget -- --test-threads=1
//! ```

#![cfg(feature = "dhat-heap")]

use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_flight::producer::{DirSource, MergeProducer};
use cqlite_flight::test_fixtures as fx;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Rows flushed into the fixture. Enough that per-row producer allocation
/// dominates fixed setup, so the budget is a meaningful producer bound.
const FIXTURE_ROWS: usize = 2_000;
const BATCH_SIZE: usize = 8192;

/// Total-bytes ceiling for one full `produce()` over the 2,000-row fixture.
/// Measured on post-#1495 (PR #2312) `main` at **~20.9 MB** (20,908,845 bytes;
/// see `benches/README.md`); ceiling = 32 MiB ≈ 1.6× measured — tight enough
/// that a materialization regression (~doubling the merged working set) fails
/// closed, loose enough to absorb allocator sizing variance. AB1/AB3/AB7 ratchet
/// this DOWN as the producer stops materializing the full result set.
const CEILING_TOTAL_BYTES: u64 = 32 * 1024 * 1024;

/// Peak-heap ceiling for the same `produce()`. Measured ~3.1 MB (3,149,012
/// bytes); ceiling = 8 MiB ≈ 2.7× measured, well under the 128 MiB project
/// budget. AB1 (Flight memory bound) tightens this toward the streaming target.
const CEILING_PEAK_BYTES: usize = 8 * 1024 * 1024;

/// Flush `FIXTURE_ROWS` `keyvalue` rows into a fresh single-SSTable fixture and
/// return `(temp_guard, data_dir)`.
fn build_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let schema = fx::keyvalue_schema();
    let temp = tempfile::TempDir::new().expect("temp dir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("write engine");
    for i in 0..FIXTURE_ROWS {
        engine
            .write(fx::keyvalue_write(&format!("k{i:06}"), &format!("v{i}")))
            .expect("write mutation");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("flush info");
    (temp, data_dir)
}

#[test]
fn producer_produce_within_memory_budget() {
    // Build + flush the fixture BEFORE the profiler so write-path allocation is
    // not attributed to the producer budget.
    let (_temp, data_dir) = build_fixture();
    let schema = fx::keyvalue_schema();
    let producer = MergeProducer::new(schema, BATCH_SIZE).expect("merge producer");
    let source = DirSource::resolve(&data_dir, fx::KEYVALUE_KS, fx::KEYVALUE_TBL, None)
        .expect("resolve fixture table dir");

    // Measure only the producer's own allocation.
    let _profiler = dhat::Profiler::builder().testing().build();
    let batches = producer.produce(&source).expect("produce must succeed");
    let stats = dhat::HeapStats::get();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Non-vacuity: the producer must have emitted rows AND allocated bytes. A
    // 0-row / 0-byte reading means the merge produced nothing — a measurement
    // failure, never a passing "0 ≤ budget".
    assert!(
        rows >= 1,
        "issue #1494: producer emitted 0 rows over a present {}-row fixture — \
         refusing a vacuous pass (the merge did not read the flushed SSTable)",
        FIXTURE_ROWS
    );
    assert!(
        stats.total_bytes > 0,
        "issue #1494: producer observed 0 allocated bytes — the produce() path \
         did not execute; refusing a vacuous pass"
    );

    eprintln!(
        "issue #1494 producer mem budget: {rows} rows -> total {} bytes, peak {} bytes \
         (ceilings: total {CEILING_TOTAL_BYTES}, peak {CEILING_PEAK_BYTES})",
        stats.total_bytes, stats.max_bytes
    );

    assert!(
        stats.total_bytes <= CEILING_TOTAL_BYTES,
        "issue #1494: Flight producer total allocation regressed to {} bytes \
         (> {CEILING_TOTAL_BYTES} ceiling) for {rows} rows. AB1/AB3/AB7 own \
         tightening this bound.",
        stats.total_bytes
    );
    assert!(
        stats.max_bytes <= CEILING_PEAK_BYTES,
        "issue #1494: Flight producer peak heap regressed to {} bytes \
         (> {CEILING_PEAK_BYTES} ceiling) for {rows} rows.",
        stats.max_bytes
    );
}
