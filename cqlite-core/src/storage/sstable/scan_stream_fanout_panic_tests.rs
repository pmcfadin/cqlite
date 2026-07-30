//! Issue #3124 — END-TO-END pins for the ≠1-generation (query-engine full scan)
//! path: a producer task that DIES mid-scan must FAIL the scan, never end it
//! cleanly with a silently short result set.
//!
//! Three boundaries, one fixture, one shape:
//!
//! * **site 1** — the fan-out k-way MERGE task ([`spawn_fanout_merge`]), the
//!   multi-generation scan's top-level producer, whose `JoinHandle` was discarded;
//! * **site 2** — a per-generation per-row SUB-SCAN task
//!   (`SSTableReader::scan_stream_admitted`), whose death the merge read as "this
//!   generation is exhausted";
//! * **site 3** — [`rechunk_into_batches`], the per-row → batch adapter behind
//!   `SSTableManager::scan_stream_batched`, which read `per_row.recv() == None` as
//!   "the scan finished".
//!
//! Each test drives the REAL public surface over a REAL multi-generation SSTable
//! directory built by the write engine, and kills the task under test with a real
//! `panic!` through the deterministic `storage::producer_fault` seam.
//!
//! # Control-arm-first, always (issue #3124 acceptance criterion 3)
//!
//! Every test FIRST drains the same fixture with NO fault armed and asserts the FULL
//! expected row count. Without that, "the faulted run is short" is vacuous: a fixture
//! that yields nothing, an arm that never fires, or a scan that took a different code
//! path would all pass. The control arm also pins that the fail-closed joins did not
//! turn a healthy scan into an error.
//!
//! # Why the arms are keyed by `(site, scope)`
//!
//! One fan-out scan runs through the merge checkpoint AND one checkpoint per
//! generation. Keyed by scope alone, a test arming site 1 would have its arm consumed
//! by whichever checkpoint the scan reached first and would then prove nothing about
//! the boundary it names. The scope itself is this test's own `TempDir` path (unique
//! per run), because this file compiles into the shared `cqlite-core` lib test binary
//! where libtest runs thousands of tests in parallel.
//!
//! Included via `#[cfg(all(test, feature = "write-support", not(feature =
//! "tombstones")))] #[path = ...] mod panic_tests;` in [`super`].

use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;

use crate::storage::sstable::SSTableManager;
use crate::platform::Platform;
use crate::storage::producer_fault::{
    arm_scan_task_panic, silence_injected_panics, ScanTaskSite, INJECTED_PANIC_MESSAGE,
};
use crate::storage::write_engine::test_support::{create_test_mutation, create_test_schema};
use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
use crate::types::TableId;
use crate::Config;

/// Partitions written per generation. Comfortably more than one batch's worth on the
/// batched surface would need, and enough that a short result is unmistakable.
const PARTITIONS_PER_GENERATION: i32 = 12;

/// Generations flushed. `> 1` is what routes `scan_stream` to the fan-out merge (the
/// ≠1-generation path this issue is about) rather than the single-reader fast path.
const GENERATIONS: i32 = 3;

/// Small enough that the producers park in backpressure mid-scan, so a fault has a
/// genuinely partial stream to truncate.
const BUFFER: usize = 2;

/// Write `GENERATIONS` flushes of `PARTITIONS_PER_GENERATION` DISJOINT partitions
/// each, so every generation contributes rows to the merge and the total row count is
/// exact and predictable.
///
/// Returns the write engine's data root (the directory an `SSTableManager` opens).
///
/// Runs on a BLOCKING thread: `WriteEngine::flush` drives its own current-thread
/// runtime, which cannot be started from inside the test's runtime.
async fn flush_generations(temp_dir: &TempDir) -> PathBuf {
    let root = temp_dir.path().to_path_buf();
    tokio::task::spawn_blocking(move || flush_generations_blocking(&root))
        .await
        .expect("fixture build task")
}

fn flush_generations_blocking(root: &std::path::Path) -> PathBuf {
    let data_dir = root.join("data");
    let config = WriteEngineConfig::new(
        data_dir.clone(),
        root.join("wal"),
        create_test_schema(),
    );
    let mut engine = WriteEngine::new(config).expect("write engine");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    for generation in 0..GENERATIONS {
        for offset in 0..PARTITIONS_PER_GENERATION {
            let id = generation * PARTITIONS_PER_GENERATION + offset;
            engine
                .write(create_test_mutation(id, &format!("row-{id}"), 1_000))
                .expect("write");
        }
        rt.block_on(engine.flush())
            .expect("flush")
            .expect("flush wrote an SSTable");
    }
    data_dir
}

/// Total rows a healthy scan of the fixture must return.
const fn expected_rows() -> usize {
    (GENERATIONS * PARTITIONS_PER_GENERATION) as usize
}

async fn open_manager(data_dir: &std::path::Path) -> SSTableManager {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableManager::new(
        data_dir,
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("manager")
}

fn table_id() -> TableId {
    let schema = create_test_schema();
    TableId::from(format!("{}.{}", schema.keyspace, schema.table).as_str())
}

/// How a drained stream ended, plus how many rows it delivered.
struct Drained {
    rows: usize,
    /// `Some(message)` when the stream terminated with an ERROR; `None` on a clean
    /// end of stream.
    error: Option<String>,
}

impl Drained {
    /// The control arm's expectations, asserted in ONE place: a healthy scan of the
    /// fixture ends cleanly AND returns every written row. Both halves matter — the
    /// count makes a later "short" assertion meaningful, and the clean end proves the
    /// #3124 joins did not make a healthy scan fail.
    fn assert_is_the_complete_control(&self, surface: &str) {
        assert_eq!(
            self.error, None,
            "{surface}: a healthy multi-generation scan must still end CLEANLY — the \
             fail-closed join must not turn a live producer into an error"
        );
        assert_eq!(
            self.rows,
            expected_rows(),
            "{surface}: test precondition — the control drain must see EVERY written \
             row, or 'the faulted drain is short' proves nothing"
        );
    }

    /// The fault arm's expectations: an ERROR naming the dead producer AND a row
    /// count strictly below the control's. A stream that merely ends early, or one
    /// that errors after delivering everything, is not the property under test.
    fn assert_failed_short(&self, surface: &str) {
        let message = self.error.as_deref().unwrap_or_else(|| {
            panic!(
                "{surface}: issue #3124 — the producer task PANICKED mid-scan, so this \
                 stream MUST terminate with an error. A clean end of stream here is a \
                 silently TRUNCATED result set served as a successful scan (got \
                 {} of {} rows and no error)",
                self.rows,
                expected_rows()
            )
        });
        assert!(
            message.contains("DIED without reporting") && message.contains("TRUNCATED"),
            "{surface}: the error must name the dead task and the truncation so the \
             failure is diagnosable, got: {message}"
        );
        assert!(
            self.rows < expected_rows(),
            "{surface}: the faulted drain returned {} of {} rows — it is not short, so \
             the fault did not truncate anything and the test would be vacuous",
            self.rows,
            expected_rows()
        );
    }
}

/// Drain the per-row surface (`SSTableManager::scan_stream`).
///
/// `schema = None` on purpose: with a schema present and `write-support` on, a
/// multi-generation read routes to the authoritative `KWayMerger`
/// (`stream_generations_for_read`), NOT the lazy fan-out merge that sites 1 and 2
/// live on. Each reader still resolves its own schema for decoding, so the rows are
/// real.
async fn drain_per_row(manager: &SSTableManager) -> Drained {
    let mut stream = match manager
        .scan_stream(&table_id(), None, None, None, BUFFER)
        .await
    {
        Ok(stream) => stream,
        Err(e) => {
            return Drained {
                rows: 0,
                error: Some(e.to_string()),
            }
        }
    };
    let mut rows = 0usize;
    while let Some(item) = stream.recv().await {
        match item {
            Ok(_) => rows += 1,
            Err(e) => {
                return Drained {
                    rows,
                    error: Some(e.to_string()),
                }
            }
        }
    }
    Drained { rows, error: None }
}

/// Drain the batched surface (`SSTableManager::scan_stream_batched`), whose
/// multi-generation arm is the [`rechunk_into_batches`] adapter — site 3.
async fn drain_batched(manager: &SSTableManager) -> Drained {
    let mut stream = match manager
        .scan_stream_batched(&table_id(), None, None, None, BUFFER)
        .await
    {
        Ok(stream) => stream,
        Err(e) => {
            return Drained {
                rows: 0,
                error: Some(e.to_string()),
            }
        }
    };
    let mut rows = 0usize;
    while let Some(item) = stream.recv().await {
        match item {
            Ok(batch) => rows += batch.len(),
            Err(e) => {
                return Drained {
                    rows,
                    error: Some(e.to_string()),
                }
            }
        }
    }
    Drained { rows, error: None }
}

/// Site 1: the fan-out k-way MERGE task dies. Pre-fix its `JoinHandle` was discarded
/// and `scan_stream` handed back the bare receiver, so the consumer saw a clean end of
/// stream and the query returned fewer rows with no error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_fanout_merge_task_fails_the_scan_instead_of_truncating_it_silently() {
    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = flush_generations(&temp_dir).await;
    let manager = open_manager(&data_dir).await;

    drain_per_row(&manager)
        .await
        .assert_is_the_complete_control("scan_stream control arm");

    let scope = temp_dir.path().to_string_lossy().to_string();
    let faulted = {
        // Silence ONLY the injected panic; restored before any assertion runs.
        let _silence = silence_injected_panics();
        let _fault = arm_scan_task_panic(&scope, ScanTaskSite::FanoutMerge);
        drain_per_row(&manager).await
    };
    faulted.assert_failed_short("scan_stream with a dead fan-out merge task");
    assert!(
        faulted
            .error
            .as_deref()
            .is_some_and(|m| m.contains(INJECTED_PANIC_MESSAGE)),
        "the error must carry the injected panic's message, proving THIS fault (not \
         some unrelated failure) is what ended the scan: {:?}",
        faulted.error
    );
}

/// Site 2: one per-generation per-row SUB-SCAN task dies. Pre-fix
/// `scan_stream_admitted` returned a bare receiver, so the merge read the dead
/// generation's closed channel as "exhausted" and completed the scan MISSING that
/// generation's remaining rows — with no error anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_per_reader_sub_scan_fails_the_fanout_instead_of_dropping_a_generation() {
    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = flush_generations(&temp_dir).await;
    let manager = open_manager(&data_dir).await;

    drain_per_row(&manager)
        .await
        .assert_is_the_complete_control("scan_stream control arm");

    let scope = temp_dir.path().to_string_lossy().to_string();
    let faulted = {
        let _silence = silence_injected_panics();
        // Taken by the FIRST sub-scan that opens over this fixture, so exactly one
        // generation's producer dies and the other generations keep producing —
        // precisely the "a generation silently vanished" shape.
        let _fault = arm_scan_task_panic(&scope, ScanTaskSite::PerRowScan);
        drain_per_row(&manager).await
    };
    faulted.assert_failed_short("scan_stream with a dead per-generation sub-scan");
}

/// Site 3: the per-row → batch re-chunker's SOURCE dies. Pre-fix `rechunk_into_batches`
/// consumed a bare receiver, so a dead per-row producer became a batch stream that
/// ended cleanly — the same silent truncation one layer up, on the surface the Flight
/// / query-engine batched consumers use.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_per_row_source_fails_the_batched_rechunk_instead_of_ending_it_cleanly() {
    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = flush_generations(&temp_dir).await;
    let manager = open_manager(&data_dir).await;

    drain_batched(&manager)
        .await
        .assert_is_the_complete_control("scan_stream_batched control arm");

    let scope = temp_dir.path().to_string_lossy().to_string();
    let faulted = {
        let _silence = silence_injected_panics();
        // Kill the per-row SOURCE the re-chunker drains. What is under test here is
        // the re-chunker's reading of that source's end of stream, not the merge.
        let _fault = arm_scan_task_panic(&scope, ScanTaskSite::FanoutMerge);
        drain_batched(&manager).await
    };
    faulted.assert_failed_short("scan_stream_batched with a dead per-row source");
}
