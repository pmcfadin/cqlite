//! Issue #3120 — END-TO-END pins: a merge producer-thread PANIC must FAIL the
//! merge, never make its run look EXHAUSTED.
//!
//! Pre-fix, the adapter mapped a channel DISCONNECT onto `None` = "this run is
//! exhausted" and neither producer shape sent a terminator, so a producer that
//! UNWOUND dropped its `SyncSender` and the merge completed SUCCESSFULLY having
//! merged only the rows that reached the channel:
//!
//! * on the READ arm, a silently short result set;
//! * on the WRITE arm, `compact_sstables` REWROTE an SSTable missing rows — silent
//!   data loss at rest.
//!
//! These drive the REAL surfaces (`SSTableRowIteratorAdapter::open`,
//! `KWayMerger::new_cancellable`, `KWayMerger::new_from_readers`,
//! `compact_sstables`) over real SSTables and real producer threads, and kill a
//! producer with a real `panic!` at a row forward via the deterministic test-only
//! seam `storage::producer_fault`.
//!
//! # Three things that make these pins non-vacuous (issue #3106's four
//! green-but-blind tests are the cautionary tale)
//!
//! 1. **CONTROL ARM FIRST, always.** Every test drains/compacts the identical
//!    fixture with NO fault armed and records the complete outcome, so "the faulted
//!    run failed / is short" is asserted against a known-good baseline rather than
//!    against nothing. A fault that never fires, or a fixture that yields no rows,
//!    then fails LOUDLY.
//! 2. **The checkpoint is reachable whatever the on-disk format.** It lives in
//!    `from_readers::forward_row`, the `emit` callback BOTH
//!    `stream_all_partitions_for_compaction` and `..._for_query` invoke, above any
//!    reader format branch — so there is no `requires_chunk_stitching()`-style
//!    bypass that could make the fault silently not fire (the exact way a #3106
//!    draft passed vacuously).
//! 3. **The arm is scoped to ONE input's `Data.db` path**, never the enclosing
//!    `TempDir`. With K inputs a TempDir-wide scope would kill whichever producer
//!    reached its first row first: a nondeterministic victim AND a
//!    nondeterministic rows-through count.
//!
//! # Why this file is IN-SRC and not in `cqlite-core/tests/`
//!
//! The gate's `write-tests` component runs `-p cqlite-core --features
//! write-support` and never enables `producer-fault-injection` (only
//! `cqlite-flight`'s dev-dependencies do), so an integration-test pin could not
//! fire. Under `cfg(test)` the arming API always exists.
//!
//! # There is NO FLUSH MERGE
//!
//! `WriteEngine::flush` → `flush_internal_async` writes the memtable straight
//! through `SSTableWriter`: no `KWayMerger`, no producer thread. The write arm is
//! COMPACTION only; a flush-arm test would be vacuous by construction, so there
//! deliberately is not one.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use tempfile::TempDir;

use super::producer_iter::RunState;
use super::producer_msg::MergeMsg;
use super::{
    channel_depth, compact_sstables, CellData, KWayMerger, MergeEntry, MergeStep, RowData,
    SSTableRowIterator, SSTableRowIteratorAdapter,
};
use crate::platform::Platform;
use crate::schema::{Column, KeyColumn, TableSchema};
use crate::storage::producer_fault::{
    arm_merge_producer_panic, silence_injected_panics, INJECTED_PANIC_MESSAGE,
};
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::write_engine::mutation::{
    CellOperation, DecoratedKey, Mutation, PartitionKey, TableId,
};
use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
use crate::types::Value;
use crate::Config;

/// Partitions per generation. Comfortably more than one row so a faulted run is
/// genuinely SHORT of the complete set rather than trivially empty on both arms.
const PARTITIONS_PER_GENERATION: i32 = 24;

/// Generations (merge inputs) — TWO, so every test drives a real multi-run merge
/// and the fault kills exactly one of two producers.
const GENERATIONS: usize = 2;

/// Rows the faulted producer forwards before it dies. Non-zero so the fault is
/// genuinely MID-walk (rows already handed over, rows still to come) rather than a
/// producer that never started.
const ROWS_BEFORE_THE_PANIC: u64 = 3;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "i3120".to_string(),
        table: "producer_panic".to_string(),
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
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

fn mutation(id: i32, generation: usize) -> Mutation {
    Mutation::new(
        TableId::new("i3120", "producer_panic"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::text(format!("g{generation}-{id}")),
        }],
        // A PINNED timestamp, never the wall clock.
        1_000_000 + (generation as i64 * 1_000) + id as i64,
        None,
    )
}

/// Flush [`GENERATIONS`] SSTables of [`PARTITIONS_PER_GENERATION`] DISJOINT
/// single-row partitions each, newest generation FIRST (the order every merger
/// constructor expects). Disjoint keys so the complete merge output row count is
/// exactly `GENERATIONS * PARTITIONS_PER_GENERATION` — a losing row from
/// reconciliation could otherwise mask a row lost to a dead producer.
fn flush_generations(temp_dir: &TempDir) -> Vec<PathBuf> {
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema(),
    );
    let mut engine = WriteEngine::new(config).expect("write engine");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let mut paths = Vec::with_capacity(GENERATIONS);
    for generation in 0..GENERATIONS {
        for row in 0..PARTITIONS_PER_GENERATION {
            let id = generation as i32 * 1_000 + row;
            engine.write(mutation(id, generation)).expect("write");
        }
        paths.push(
            rt.block_on(engine.flush())
                .expect("flush")
                .expect("flush wrote an SSTable")
                .data_path,
        );
    }
    // Newest generation first.
    paths.reverse();
    paths
}

/// The complete row count a healthy merge over the fixture must produce.
fn complete_row_count() -> usize {
    GENERATIONS * PARTITIONS_PER_GENERATION as usize
}

/// Arm the fault for exactly ONE input (never the whole `TempDir`) — see the
/// module doc. The `Data.db` path is unique per run, so no concurrently-running
/// test can consume this arm and no sibling can clobber it.
fn scope_of(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

fn open_reader(path: &Path) -> Arc<SSTableReader> {
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

/// How a drained merger ended, plus how many rows it delivered.
struct Drained {
    rows: usize,
    /// `Some(message)` when the merge terminated with an ERROR; `None` when it
    /// reported a clean `MergeStep::Complete`.
    error: Option<String>,
}

fn drain(mut merger: KWayMerger) -> Drained {
    let mut rows = 0;
    loop {
        match merger.step() {
            Ok(MergeStep::Partition { rows: r, .. }) => rows += r.len(),
            Ok(MergeStep::Complete) => return Drained { rows, error: None },
            Err(e) => {
                return Drained {
                    rows,
                    error: Some(e.to_string()),
                }
            }
        }
    }
}

/// Every fail-closed error must NAME the panic, carry its payload, and say the run
/// is truncated — the difference between a debuggable failure and a mystery.
fn assert_names_the_dead_producer(message: &str) {
    assert!(
        message.contains("PANICKED") && message.contains(INJECTED_PANIC_MESSAGE),
        "the error must name the panic and carry its message, got: {message}"
    );
    assert!(
        message.contains("TRUNCATED"),
        "the error must state the run is incomplete, got: {message}"
    );
}

/// ADAPTER-level pin (issue #3120): the run whose producer PANICKED must report an
/// ERROR, and that verdict must be STICKY — a repeat poll may never decay into the
/// `None` that means "this run is exhausted".
///
/// The stickiness half is asserted HERE rather than through `KWayMerger` because
/// `RunReader::refill_buffer` propagates our `Err` but keeps no sticky error of its
/// own, so the adapter is the only place the property can be observed directly. A
/// consumer that swallowed the first error and advanced again would otherwise get a
/// dead producer downgraded to a clean end of input.
#[test]
fn a_dead_producer_run_reports_an_error_that_never_decays_to_end_of_input() {
    let temp_dir = TempDir::new().expect("tempdir");
    let paths = flush_generations(&temp_dir);
    let schema = schema();

    // Control arm: an unfaulted run yields every partition and THEN reports
    // end-of-input, proven by the producer's `Done` terminator.
    let mut healthy = SSTableRowIteratorAdapter::open(
        &paths[0],
        0,
        &schema,
        None,
        ScanCancel::default(),
        super::STREAMING_CHANNEL_CAPACITY,
    )
    .expect("adapter opens");
    let mut healthy_rows = 0;
    loop {
        match healthy.next() {
            Some(Ok(_)) => healthy_rows += 1,
            Some(Err(e)) => panic!("a healthy run must not error: {e}"),
            None => break,
        }
    }
    assert_eq!(
        healthy_rows, PARTITIONS_PER_GENERATION as usize,
        "test precondition: the control run must see EVERY written partition, or \
         'the faulted run is short' would be vacuous"
    );
    assert!(
        healthy.next().is_none(),
        "a run PROVEN finished by the Done terminator stays finished"
    );
    drop(healthy);

    // Fault arm: the producer panics after `ROWS_BEFORE_THE_PANIC` forwards.
    let (faulted_rows, first_error, second_error) = {
        let _silence = silence_injected_panics();
        let _fault = arm_merge_producer_panic(&scope_of(&paths[0]), ROWS_BEFORE_THE_PANIC);
        let mut faulted = SSTableRowIteratorAdapter::open(
            &paths[0],
            0,
            &schema,
            None,
            ScanCancel::default(),
            super::STREAMING_CHANNEL_CAPACITY,
        )
        .expect("adapter opens");
        let mut rows = 0;
        let first = loop {
            match faulted.next() {
                Some(Ok(_)) => rows += 1,
                Some(Err(e)) => break Some(e.to_string()),
                None => break None,
            }
        };
        // The SECOND poll after the verdict: this is the sticky-state pin.
        let second = match faulted.next() {
            Some(Ok(_)) => Some("<unexpected row>".to_string()),
            Some(Err(e)) => Some(e.to_string()),
            None => None,
        };
        (rows, first, second)
    };

    let first_error = first_error.expect(
        "a producer thread that PANICKED mid-walk must terminate this run with an \
         ERROR — reporting end-of-input here is issue #3120: a silently short read, \
         or a REWRITTEN SSTable missing rows",
    );
    assert_names_the_dead_producer(&first_error);
    assert_eq!(
        faulted_rows, ROWS_BEFORE_THE_PANIC as usize,
        "exactly the rows forwarded before the fault reach the consumer"
    );
    assert!(
        faulted_rows < healthy_rows,
        "the faulted run MUST be short of the complete {healthy_rows} rows — \
         otherwise the fault never fired and this test proves nothing"
    );
    let second_error = second_error.expect(
        "the dead-producer verdict must be STICKY: a repeat poll that returns None \
         hands a consumer which swallowed the first error a CLEAN end-of-input for a \
         truncated run (issue #3120)",
    );
    assert_names_the_dead_producer(&second_error);
}

/// READ arm, path-based merger (`KWayMerger::new_cancellable`): a dead producer
/// must fail the merge and never let it report `MergeStep::Complete` with a short
/// row set.
#[test]
fn the_path_based_merger_fails_when_a_producer_dies_instead_of_returning_short() {
    let temp_dir = TempDir::new().expect("tempdir");
    let paths = flush_generations(&temp_dir);
    let schema = schema();

    let complete = drain(
        KWayMerger::new_cancellable(paths.clone(), &schema, ScanCancel::default())
            .expect("control merger constructs"),
    );
    assert_eq!(
        complete.error, None,
        "a healthy merge must still complete cleanly, not error"
    );
    assert_eq!(
        complete.rows,
        complete_row_count(),
        "test precondition: the control drain must reconcile EVERY written row"
    );

    let faulted = {
        let _silence = silence_injected_panics();
        let _fault = arm_merge_producer_panic(&scope_of(&paths[0]), ROWS_BEFORE_THE_PANIC);
        drain(
            KWayMerger::new_cancellable(paths.clone(), &schema, ScanCancel::default())
                .expect("faulted merger constructs"),
        )
    };

    let message = faulted.error.expect(
        "a merge whose producer PANICKED must FAIL — reporting MergeStep::Complete \
         with a short row set is issue #3120",
    );
    assert_names_the_dead_producer(&message);
    assert!(
        faulted.rows < complete.rows,
        "the faulted merge MUST be short of the complete {} rows — otherwise the \
         fault never fired",
        complete.rows
    );
}

/// READ arm, SHARED-READER merger (`KWayMerger::new_from_readers`, the warm
/// `do_get` shape): the same property over the OTHER producer shape, whose emit
/// funnel is `drive_query_stream` rather than `drive_compaction_stream`. One
/// checkpoint covers both.
#[test]
fn the_shared_reader_merger_fails_when_a_producer_dies() {
    let temp_dir = TempDir::new().expect("tempdir");
    let paths = flush_generations(&temp_dir);
    let schema = schema();
    let readers: Vec<Arc<SSTableReader>> = paths.iter().map(|p| open_reader(p)).collect();

    let complete = drain(
        KWayMerger::new_from_readers(readers.clone(), &schema, ScanCancel::default(), None)
            .expect("control merger constructs"),
    );
    assert_eq!(
        complete.error, None,
        "a healthy shared-reader merge must complete cleanly"
    );
    assert_eq!(
        complete.rows,
        complete_row_count(),
        "test precondition: the control drain must reconcile EVERY written row"
    );

    let faulted = {
        let _silence = silence_injected_panics();
        let _fault = arm_merge_producer_panic(&scope_of(&paths[0]), ROWS_BEFORE_THE_PANIC);
        drain(
            KWayMerger::new_from_readers(readers.clone(), &schema, ScanCancel::default(), None)
                .expect("faulted merger constructs"),
        )
    };

    let message = faulted
        .error
        .expect("a shared-reader merge whose producer PANICKED must FAIL (issue #3120)");
    assert_names_the_dead_producer(&message);
    assert!(
        faulted.rows < complete.rows,
        "the faulted merge MUST be short of the complete {} rows — otherwise the \
         fault never fired and this test proves nothing",
        complete.rows
    );
}

/// WRITE arm — THE data-loss claim (issue #3120): a compaction whose producer
/// thread PANICKED must return `Err` and PUBLISH NOTHING. It must never emit a
/// short-but-`Ok` rewritten SSTable, because compaction supersedes (and the
/// background path deletes) its inputs, so a missing row is gone for good.
///
/// `TOC.txt` is the publication barrier the writer emits LAST, so its absence is
/// the authoritative "nothing was published" signal.
#[test]
fn a_compaction_whose_producer_dies_fails_and_publishes_no_output() {
    let temp_dir = TempDir::new().expect("tempdir");
    let paths = flush_generations(&temp_dir);
    let schema = schema();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    // Control arm: the identical compaction call, unfaulted.
    let control_out = temp_dir.path().join("out-control");
    std::fs::create_dir_all(&control_out).expect("mkdir");
    let control = rt
        .block_on(compact_sstables(
            paths.clone(),
            &control_out,
            &schema,
            10,
            None,
            None,
            false,
        ))
        .expect("the control compaction must succeed");
    assert_eq!(
        control.stats.output_rows as usize,
        complete_row_count(),
        "test precondition: the control compaction must write EVERY row, or \
         'the faulted compaction lost rows' would be vacuous"
    );
    assert!(
        control.output.toc_path.exists(),
        "the control compaction must PUBLISH its output (TOC.txt present)"
    );

    // Fault arm: identical call, one input's producer dies mid-walk.
    let faulted_out = temp_dir.path().join("out-faulted");
    std::fs::create_dir_all(&faulted_out).expect("mkdir");
    let faulted = {
        let _silence = silence_injected_panics();
        let _fault = arm_merge_producer_panic(&scope_of(&paths[0]), ROWS_BEFORE_THE_PANIC);
        rt.block_on(compact_sstables(
            paths.clone(),
            &faulted_out,
            &schema,
            11,
            None,
            None,
            false,
        ))
    };

    let message = faulted
        .expect_err(
            "a compaction whose producer PANICKED must return Err — a short-but-Ok \
             CompactReport means an SSTable was REWRITTEN MISSING ROWS and its \
             inputs superseded: silent data loss at rest (issue #3120)",
        )
        .to_string();
    assert_names_the_dead_producer(&message);

    let published: Vec<PathBuf> = std::fs::read_dir(&faulted_out)
        .expect("read output dir")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("TOC.txt"))
        })
        .collect();
    assert!(
        published.is_empty(),
        "a failed compaction must PUBLISH NOTHING — TOC.txt is the publication \
         barrier and must be absent, found: {published:?}"
    );
}

/// Rows the hand-built producer delivers before it dies UNREPORTABLY, so the bare
/// disconnect is genuinely a MID-run truncation and not an empty run.
const ROWS_BEFORE_THE_BARE_DISCONNECT: usize = 3;

/// A `MergeEntry` for the hand-built producer below. Content is irrelevant — only
/// the message VARIANT matters to the protocol under test.
fn synthetic_entry(n: i64) -> MergeEntry {
    MergeEntry::new(
        0,
        DecoratedKey::new(n, n.to_be_bytes().to_vec()),
        None,
        100,
        RowData::Live {
            cells: vec![CellData::new("name".to_string(), Value::text("v"), 100)],
        },
    )
}

/// THE regression guard for the literal line that held the P0 defect (roborev,
/// issue #3120): `RecvTimeoutError::Disconnected`. Revert that arm to `return None`
/// and THIS test — and only this test — goes red.
///
/// # Why the four injected-fault tests above cannot cover this arm
///
/// The fix's `catch_unwind` is *too* effective for them to reach it: every panic it
/// catches is converted into a proper `MergeMsg::Failed` TERMINATOR, so those tests
/// exercise the `Failed` arm and the channel never disconnects terminator-less.
/// Closing the front door made the back door unreachable from the front-door tests.
/// A bare disconnect is still reachable in production by a death the producer cannot
/// report at all: a process-level `abort`, a double panic, `panic = "abort"`, or a
/// panic *inside the terminal `send` itself*. That is the case this pins.
///
/// So the producer is hand-built rather than fault-injected — deliberately BYPASSING
/// the terminator protocol, which is the whole point. It delivers
/// `ROWS_BEFORE_THE_BARE_DISCONNECT` entries (accounted on the egress-depth gauge
/// exactly as `from_readers::forward_row` does, so the `Drop` reconcile's
/// `debug_assert!(residual >= 0)` sees a truthful sent/received pair) and then drops
/// its sender with NO `Done` and NO `Failed`.
///
/// Two assertions, both load-bearing:
/// 1. the FIRST poll after the disconnect is the dead-producer ERROR, not `None` —
///    `None` there is the silent truncation this P0 exists to eliminate;
/// 2. a REPEAT poll returns the SAME sticky error, never `None` — separately
///    load-bearing because `RunReader::refill_buffer` propagates our `Err` but keeps
///    no sticky error of its own, so a consumer that swallowed the first one and
///    advanced again would otherwise get a clean end-of-input for a truncated run.
#[test]
fn a_producer_that_disconnects_without_a_terminator_is_an_error_not_end_of_input() {
    let (sender, receiver) =
        std::sync::mpsc::sync_channel::<MergeMsg>(super::STREAMING_CHANNEL_CAPACITY);
    let sent_count = Arc::new(AtomicI64::new(0));
    let producer_sent_count = sent_count.clone();

    // The "unreportable death": hand a few entries over, then drop the sender
    // WITHOUT a terminator. No `producer_gauge::ProducerThreadGuard` is created
    // here, so the live-producer gauge is untouched (this thread was never
    // accounted as spawned).
    let producer = std::thread::Builder::new()
        .spawn(move || {
            for n in 0..ROWS_BEFORE_THE_BARE_DISCONNECT as i64 {
                let msg = MergeMsg::Item(synthetic_entry(n));
                let is_data = msg.is_tracked_data();
                if sender.send(msg).is_ok() && is_data {
                    channel_depth::sent();
                    producer_sent_count.fetch_add(1, Ordering::SeqCst);
                }
            }
            drop(sender);
        })
        .expect("spawn hand-built producer");

    let mut adapter = SSTableRowIteratorAdapter {
        receiver: Some(receiver),
        producer: Some(producer),
        scan_cancel: ScanCancel::default(),
        sent_count,
        received_count: 0,
        state: RunState::Streaming,
        egress_channel_capacity: super::STREAMING_CHANNEL_CAPACITY,
    };

    let mut rows = 0;
    let first = loop {
        match adapter.next() {
            Some(Ok(_)) => rows += 1,
            Some(Err(e)) => break Some(e.to_string()),
            None => break None,
        }
    };
    let second = match adapter.next() {
        Some(Ok(_)) => Some("<unexpected row>".to_string()),
        Some(Err(e)) => Some(e.to_string()),
        None => None,
    };

    assert_eq!(
        rows, ROWS_BEFORE_THE_BARE_DISCONNECT,
        "test precondition: every entry handed over before the disconnect must reach \
         the consumer, or 'the run is truncated' would be vacuous"
    );
    let first = first.expect(
        "a sender dropped WITHOUT a terminator can only mean the producer died in a \
         way it could not report, so this run is TRUNCATED — returning end-of-input \
         here is the literal issue #3120 defect: a silently short read, or an \
         SSTable REWRITTEN missing rows",
    );
    assert!(
        first.contains("WITHOUT a terminal Done") && first.contains("TRUNCATED"),
        "the error must name the missing terminator and say the run is incomplete, \
         got: {first}"
    );
    let second = second.expect(
        "the Died verdict must be STICKY: a repeat poll that returns None hands a \
         consumer which swallowed the first error a CLEAN end-of-input for a \
         truncated run (issue #3120)",
    );
    assert_eq!(
        second, first,
        "the sticky verdict must be the IDENTICAL error, not a different one"
    );
}
