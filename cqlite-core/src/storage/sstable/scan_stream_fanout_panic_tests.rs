//! Issue #3124 — END-TO-END pins for the ≠1-generation (query-engine full scan)
//! path: a producer task that DIES mid-scan must FAIL the scan, never end it
//! cleanly with a silently short result set.
//!
//! Four boundaries, two fixtures, one shape:
//!
//! * **site 1** — the fan-out k-way MERGE task ([`spawn_fanout_merge`]), the
//!   multi-generation scan's top-level producer, whose `JoinHandle` was discarded;
//! * **site 2** — a per-generation per-row SUB-SCAN task
//!   (`SSTableReader::scan_stream_admitted`), whose death the merge read as "this
//!   generation is exhausted";
//! * **site 3** — [`rechunk_into_batches`], the per-row → batch adapter behind
//!   `SSTableManager::scan_stream_batched`, which read `per_row.recv() == None` as
//!   "the scan finished";
//! * **site 5** — the CROSS-GENERATION reconciling merge task
//!   (`generation_merge::stream_generations_for_read`), whose death was flattened into
//!   a plain setup error and then answered by FALLING BACK to the non-reconciling
//!   concat — see the second fixture below. (Site 4, the windowed forwarder, has its
//!   own end-to-end file: it needs a compressed reader.)
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
//! # Why site 5 needs its OWN, OVERLAPPING fixture
//!
//! Sites 1–3 write DISJOINT partitions per generation, where the reconciled and the
//! concatenated result sets are the SAME 36 rows — fine when the property is "short vs
//! complete", useless when the property is "reconciled vs unreconciled". Site 5's
//! failure mode was a FULL-LENGTH but WRONG result set (the concat fallback), so its
//! fixture rewrites the SAME partitions in every generation: 12 reconciled rows
//! (newest-generation values) versus 36 unreconciled ones. The test can therefore tell
//! the two apart by BOTH count and value, which a disjoint fixture cannot.
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

use crate::platform::Platform;
use crate::storage::producer_fault::{
    arm_scan_task_panic, silence_injected_panics, ScanTaskSite, INJECTED_PANIC_MESSAGE,
};
use crate::storage::sstable::SSTableManager;
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
    let config = WriteEngineConfig::new(data_dir.clone(), root.join("wal"), create_test_schema());
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

/// Total rows a healthy scan of the DISJOINT fixture must return.
const fn expected_rows() -> usize {
    (GENERATIONS * PARTITIONS_PER_GENERATION) as usize
}

/// Write `GENERATIONS` flushes that all rewrite the SAME `PARTITIONS_PER_GENERATION`
/// partitions, each generation with a strictly newer timestamp and a distinguishable
/// value — the OVERLAPPING fixture site 5 needs (see the header).
///
/// A reconciling read returns [`reconciled_rows`] rows, all carrying
/// [`newest_value_prefix`]; the non-reconciling concat returns [`unreconciled_rows`]
/// rows, including every older generation's superseded copy. That gap is what lets the
/// site-5 test assert "it did not silently fall back to the concat".
async fn flush_overlapping_generations(temp_dir: &TempDir) -> PathBuf {
    let root = temp_dir.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let data_dir = root.join("data");
        let config =
            WriteEngineConfig::new(data_dir.clone(), root.join("wal"), create_test_schema());
        let mut engine = WriteEngine::new(config).expect("write engine");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        for generation in 0..GENERATIONS {
            for id in 0..PARTITIONS_PER_GENERATION {
                engine
                    .write(create_test_mutation(
                        id,
                        &format!("gen{generation}-row-{id}"),
                        // Strictly increasing, so the LWW winner is unambiguous and
                        // the newest generation is the one a reconciled read shows.
                        1_000 + generation as i64,
                    ))
                    .expect("write");
            }
            rt.block_on(engine.flush())
                .expect("flush")
                .expect("flush wrote an SSTable");
        }
        data_dir
    })
    .await
    .expect("fixture build task")
}

/// Rows a RECONCILED read of the overlapping fixture returns: last-write-wins collapses
/// every generation's copy of a partition into one row.
const fn reconciled_rows() -> usize {
    PARTITIONS_PER_GENERATION as usize
}

/// Rows the NON-reconciling token-order concat returns over the same fixture: every
/// generation's copy of every partition, superseded ones included.
const fn unreconciled_rows() -> usize {
    (GENERATIONS * PARTITIONS_PER_GENERATION) as usize
}

/// The value prefix only the NEWEST generation's cells carry. Every row of a reconciled
/// read must have it; a concatenated read also surfaces older prefixes.
fn newest_value_prefix() -> String {
    format!("gen{}-", GENERATIONS - 1)
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

/// Drain the per-row surface WITH a schema, capturing every row's `name` value.
///
/// The schema is what routes a multi-generation read to the authoritative RECONCILING
/// `KWayMerger` (`stream_generations_for_read`) instead of the lazy concat — i.e. it is
/// what puts site 5 on the path at all. The captured values are what let the caller tell
/// a reconciled result set from a concatenated one, which a row COUNT alone cannot do
/// once a fallback returns a full-length answer.
async fn drain_reconciled(manager: &SSTableManager) -> (Drained, Vec<String>) {
    let schema = create_test_schema();
    let mut stream = match manager
        .scan_stream(&table_id(), None, None, Some(&schema), BUFFER)
        .await
    {
        Ok(stream) => stream,
        Err(e) => {
            return (
                Drained {
                    rows: 0,
                    error: Some(e.to_string()),
                },
                Vec::new(),
            )
        }
    };
    let mut rows = 0usize;
    let mut values = Vec::new();
    while let Some(item) = stream.recv().await {
        match item {
            Ok((_, row)) => {
                rows += 1;
                if let crate::types::ScanRow::Row(cells) = row {
                    for (column, value) in cells.iter() {
                        if column.as_ref() == "name" {
                            values.push(value.as_str().unwrap_or_default().to_string());
                        }
                    }
                }
            }
            Err(e) => {
                return (
                    Drained {
                        rows,
                        error: Some(e.to_string()),
                    },
                    values,
                )
            }
        }
    }
    (Drained { rows, error: None }, values)
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

/// Site 5: the CROSS-GENERATION reconciling merge task dies during construction.
///
/// Pre-fix this was the WORST of the #3124 family, because the consumer did not merely
/// mistake it for a clean end of stream — it mistook it for "the merger could not be
/// CONSTRUCTED" and answered by falling back to `spawn_fanout_merge`, the
/// non-reconciling token-order CONCAT. The caller then got a FULL-LENGTH, UNRECONCILED
/// result set (every generation's superseded copy of every partition) with `Ok` and a
/// `tracing::warn!`: silently WRONG data rather than silently SHORT data.
///
/// So this test pins BOTH halves, and needs the overlapping fixture to do it:
///
/// 1. the query FAILS (a dead producer is never a successful scan), and
/// 2. it did NOT fall back — the drain is not the unreconciled row set, by count and by
///    value.
///
/// The control arm first proves the healthy read really is RECONCILED (12 rows, all
/// newest-generation values), so half 2 is a meaningful assertion rather than a
/// tautology over a fixture where both answers coincide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_cross_generation_merge_fails_the_scan_instead_of_falling_back_to_the_concat() {
    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = flush_overlapping_generations(&temp_dir).await;
    let manager = open_manager(&data_dir).await;

    // Test precondition: the fixture must make the two answers distinguishable at all.
    assert_ne!(
        reconciled_rows(),
        unreconciled_rows(),
        "test precondition: with reconciled == unreconciled this test could not tell a \
         fail-closed read from a silent concat fallback"
    );

    // Control arm: the healthy read ends cleanly, is RECONCILED (one row per partition)
    // and shows the newest generation's values.
    let (control, control_values) = drain_reconciled(&manager).await;
    assert_eq!(
        control.error, None,
        "a healthy multi-generation reconciling read must still end CLEANLY — the \
         fail-closed join must not turn a live producer into an error"
    );
    assert_eq!(
        control.rows,
        reconciled_rows(),
        "the control drain must be RECONCILED: {} partitions rewritten in every \
         generation collapse to {} rows, not the concat's {}",
        PARTITIONS_PER_GENERATION,
        reconciled_rows(),
        unreconciled_rows()
    );
    let newest = newest_value_prefix();
    assert!(
        control_values.len() == reconciled_rows()
            && control_values.iter().all(|v| v.starts_with(&newest)),
        "every reconciled row must carry the newest generation's ({newest}) value — \
         otherwise the control is not the LWW winner set and 'did not fall back' below \
         proves nothing, got: {control_values:?}"
    );

    let scope = temp_dir.path().to_string_lossy().to_string();
    let (faulted, faulted_values) = {
        // Silence ONLY the injected panic; restored before any assertion runs.
        let _silence = silence_injected_panics();
        let _fault = arm_scan_task_panic(&scope, ScanTaskSite::CrossGenerationMerge);
        drain_reconciled(&manager).await
    };

    // Half 1: it FAILED, and the error says which producer died and that the fallback
    // was refused on purpose.
    let message = faulted.error.as_deref().unwrap_or_else(|| {
        panic!(
            "issue #3124: the cross-generation merge producer PANICKED, so this read \
             MUST fail. Completing it means the concat fallback ran: {} rows of \
             UNRECONCILED data (duplicated overwritten rows, resurrected deleted ones) \
             served as a successful reconciling scan (got {} rows: {faulted_values:?})",
            unreconciled_rows(),
            faulted.rows
        )
    });
    assert!(
        message.contains("DIED without reporting")
            && message.contains("CANNOT fall back")
            && message.contains(INJECTED_PANIC_MESSAGE),
        "the error must name the dead producer, say the concat fallback is refused, and \
         carry THIS fault's panic message (not some unrelated failure), got: {message}"
    );

    // Half 2: it did NOT fall back — neither the unreconciled row COUNT nor any
    // superseded older-generation VALUE reached the caller. The value half matters
    // independently: a fallback that happened to return a coinciding count would still
    // be caught here.
    assert_ne!(
        faulted.rows,
        unreconciled_rows(),
        "the faulted read returned exactly the {} rows the NON-reconciling concat \
         produces — the fallback ran and served wrong data under an error",
        unreconciled_rows()
    );
    assert!(
        faulted_values.iter().all(|v| v.starts_with(&newest)),
        "the faulted read surfaced a superseded older-generation value, which only the \
         non-reconciling concat can produce: {faulted_values:?}"
    );
}
