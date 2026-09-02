//! Issue #3106 — END-TO-END pin: a producer-thread PANIC mid-walk must FAIL the
//! query row stream, never complete it with a silently truncated result set.
//!
//! This drives the REAL surface — `SSTableReader::open_query_row_stream` over a
//! real SSTable, a real Summary-guided/full-index walk, a real detached producer
//! thread — and kills that thread with a real `panic!` at a batch boundary via the
//! deterministic test-only seam `storage::producer_fault`. A channel-level unit
//! test (`query_rows_tests.rs`) cannot prove this: it never runs a producer
//! thread, so it cannot show that an UNWINDING walk is what the consumer now
//! reports as an error.
//!
//! The control arm matters as much as the fault arm: the same fixture is drained
//! with NO fault armed first, so "the stream failed" is asserted against a known
//! complete row count rather than against nothing (a fault that never fired, or a
//! fixture that yields no rows, would otherwise pass vacuously).
//!
//! Included via `#[cfg(all(test, feature = "write-support"))] #[path = ...] mod
//! panic_tests;` — the fixture is built with the write engine, and the file is
//! separate to keep `query_rows.rs` under the campsite-rule size limit (#1116).

use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;

use super::*;
use crate::platform::Platform;
use crate::storage::producer_fault::{
    arm_query_row_producer_panic, silence_injected_panics, INJECTED_PANIC_MESSAGE,
};
use crate::storage::write_engine::test_support::{create_test_mutation, create_test_schema};
use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
use crate::Config;

/// Partitions in the fixture. Must exceed [`QUERY_ROWS_PER_BATCH`] by enough that
/// the walk hands over SEVERAL batches, so a fault armed after the first one kills
/// the producer genuinely MID-stream (rows already delivered, rows still to come)
/// — which is the case that used to be reported as a successful, complete scan.
const PARTITIONS: i32 = 3 * QUERY_ROWS_PER_BATCH as i32;

/// Batches the consumer is allowed to receive before the producer dies.
const BATCHES_BEFORE_THE_PANIC: u64 = 1;

/// A pinned read-time clock: never the wall clock, so TTL/expiry decisions in the
/// walk are reproducible (this fixture writes no TTLs, but the pin is the rule).
const NOW_SECS: i64 = 1_700_000_000;

/// Write `PARTITIONS` single-row partitions and flush them as ONE SSTable
/// generation, returning its `Data.db` path.
fn flush_one_generation(temp_dir: &TempDir) -> PathBuf {
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    );
    let mut engine = WriteEngine::new(config).expect("write engine");
    for id in 0..PARTITIONS {
        engine
            .write(create_test_mutation(id, &format!("row-{id}"), 1_000))
            .expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("flush wrote an SSTable")
        .data_path
}

fn open_reader(path: &std::path::Path) -> Arc<SSTableReader> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        Arc::new(
            SSTableReader::open(path, &config, platform)
                .await
                .expect("open reader"),
        )
    })
}

/// The FULL-RING token bound (`start == end`, per `ScanTokenBound::contains`).
///
/// Passing `Some(..)` selects the token-bounded arm, whose `BatchSink` batches at
/// `QUERY_ROWS_PER_BATCH`; the bound itself excludes nothing, so the control arm
/// still sees every partition.
fn full_ring() -> ScanTokenBound {
    ScanTokenBound {
        start_excl: i64::MIN,
        end_incl: i64::MIN,
    }
}

fn open_stream(reader: &Arc<SSTableReader>) -> QueryRowStream {
    Arc::clone(reader)
        .open_query_row_stream(
            create_test_schema(),
            Some(full_ring()),
            NOW_SECS,
            ScanCancel::new(),
        )
        .expect("open query row stream")
}

/// How a drained stream ended, plus how many rows it delivered.
struct Drained {
    rows: usize,
    /// `Some(message)` when the stream terminated with an ERROR; `None` when it
    /// reported a clean end of stream.
    error: Option<String>,
}

fn drain(mut stream: QueryRowStream) -> Drained {
    let mut rows = 0;
    loop {
        match stream.next_batch() {
            None => return Drained { rows, error: None },
            Some(Ok(QueryRowBatch::Rows(batch))) => rows += batch.len(),
            Some(Ok(QueryRowBatch::Unsupported)) => {
                panic!(
                    "test precondition: this reader must be servable by the \
                     single-generation walk, but it reported Unsupported"
                )
            }
            Some(Err(e)) => {
                return Drained {
                    rows,
                    error: Some(e.to_string()),
                }
            }
        }
    }
}

/// The pin (issue #3106): with the producer thread killed mid-walk, the stream
/// must terminate with an ERROR and a SHORT row count — not the clean end of
/// stream that made a truncated result set indistinguishable from a complete one.
#[test]
fn a_producer_panic_mid_stream_fails_the_stream_instead_of_truncating_it_silently() {
    let temp_dir = TempDir::new().expect("tempdir");
    let data_path = flush_one_generation(&temp_dir);
    let reader = open_reader(&data_path);

    // Control arm: no fault armed. Establishes the COMPLETE row count and that a
    // healthy walk still reports a clean end of stream (the `Done` sentinel).
    let complete = drain(open_stream(&reader));
    assert_eq!(
        complete.error, None,
        "a healthy walk must still end cleanly (the Done sentinel), not error"
    );
    assert_eq!(
        complete.rows, PARTITIONS as usize,
        "test precondition: the control drain must see EVERY written partition, \
         or 'the faulted drain is short' would be vacuous"
    );

    // Fault arm: the producer thread panics just before its second batch handoff.
    //
    // The arm is SCOPED to this test's own `TempDir` path (issue #3106, roborev):
    // this file compiles into the shared `cqlite-core` lib test binary, where
    // libtest runs thousands of tests in parallel, so an UNSCOPED process-global
    // arm could be consumed by a concurrent test's stream — and this test would
    // then pass for the wrong reason. A `TempDir` path is unique per run, so no
    // other reader can match it and no sibling can clobber the registration.
    let scope = temp_dir.path().to_string_lossy().to_string();
    let faulted = {
        // Silence ONLY the injected panic's console noise, and restore the
        // previous hook before any assertion below runs.
        let _silence = silence_injected_panics();
        let _fault = arm_query_row_producer_panic(&scope, BATCHES_BEFORE_THE_PANIC);
        drain(open_stream(&reader))
    };

    let message = faulted.error.expect(
        "a producer thread that PANICKED mid-walk must terminate the stream with \
         an ERROR — reporting a clean end of stream here is issue #3106, a \
         silently truncated result set served as a successful scan",
    );
    assert!(
        message.contains("PANICKED") && message.contains(INJECTED_PANIC_MESSAGE),
        "the error must name the panic and carry its message so the failure is \
         debuggable rather than a generic 'the producer died', got: {message}"
    );
    assert!(
        message.contains("TRUNCATED"),
        "the error must state that the result set is incomplete, got: {message}"
    );
    assert_eq!(
        faulted.rows,
        BATCHES_BEFORE_THE_PANIC as usize * QUERY_ROWS_PER_BATCH,
        "exactly the batches handed over before the fault reach the consumer"
    );
    assert!(
        faulted.rows < complete.rows,
        "the faulted drain MUST be short of the complete {} rows — otherwise the \
         fault never fired and this test proves nothing",
        complete.rows
    );
}
