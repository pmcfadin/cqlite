//! Shared TEST fixture for the multi-generation streaming read path: real SSTable
//! generations written by the write engine, plus the drains and the reconciled /
//! unreconciled ORACLE both end-to-end suites assert against.
//!
//! Two suites share it on purpose, so they cannot drift apart on what "reconciled"
//! means:
//!
//! * `sstable::scan_stream_fanout::panic_tests` (issue #3124) — a producer task that
//!   DIES must fail the scan instead of truncating it silently, and, at site 5, must
//!   not be answered with the concat.
//! * `generation_merge::setup_fail_closed_tests` (issue #3154) — a `KWayMerger::new`
//!   failure earns the concat fallback ONLY when the input is genuinely
//!   merger-ineligible; an I/O or corruption failure propagates.
//!
//! # The OVERLAPPING fixture is what makes either property provable
//!
//! [`flush_generations`] writes DISJOINT partitions per generation, where the
//! reconciled and the concatenated result sets are the SAME rows — fine when the
//! property is "short vs complete", useless when it is "reconciled vs unreconciled".
//! [`flush_overlapping_generations`] rewrites the SAME partitions in every generation
//! with strictly increasing timestamps, so a reconciling read returns
//! [`reconciled_rows`] rows all carrying [`newest_value_prefix`], while the
//! non-reconciling concat returns [`unreconciled_rows`] rows including every
//! superseded copy. The two answers therefore differ by COUNT and by VALUE, which is
//! the only way to tell a silent concat fallback from a genuine reconciled read.
//!
//! Gated `cfg(test)` + `not(tombstones)`: a `tombstones` build routes `scan_stream`
//! through the materializing `scan`, so neither streaming path under test exists
//! there. (`write-support` comes from the enclosing `generation_merge` module.)

use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;

use crate::platform::Platform;
use crate::storage::sstable::SSTableManager;
use crate::storage::write_engine::test_support::{create_test_mutation, create_test_schema};
use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
use crate::types::TableId;
use crate::Config;

/// Partitions written per generation. Comfortably more than one batch's worth on
/// the batched surface would need, and enough that a short result is unmistakable.
pub(in crate::storage::sstable) const PARTITIONS_PER_GENERATION: i32 = 12;

/// Generations flushed. `> 1` is what routes `scan_stream` to a multi-generation
/// merge rather than the single-reader fast path.
pub(in crate::storage::sstable) const GENERATIONS: i32 = 3;

/// Small enough that the producers park in backpressure mid-scan, so a fault has a
/// genuinely partial stream to truncate.
pub(in crate::storage::sstable) const BUFFER: usize = 2;

/// Write `GENERATIONS` flushes of `PARTITIONS_PER_GENERATION` DISJOINT partitions
/// each, so every generation contributes rows to the merge and the total row count
/// is exact and predictable.
///
/// Returns the write engine's data root (the directory an `SSTableManager` opens).
///
/// Runs on a BLOCKING thread: `WriteEngine::flush` drives its own current-thread
/// runtime, which cannot be started from inside the test's runtime.
pub(in crate::storage::sstable) async fn flush_generations(temp_dir: &TempDir) -> PathBuf {
    let root = temp_dir.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        with_engine(&root, |engine, rt| {
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
        })
    })
    .await
    .expect("fixture build task")
}

/// Total rows a healthy scan of the DISJOINT fixture must return.
pub(in crate::storage::sstable) const fn expected_rows() -> usize {
    (GENERATIONS * PARTITIONS_PER_GENERATION) as usize
}

/// Write `GENERATIONS` flushes that all rewrite the SAME
/// `PARTITIONS_PER_GENERATION` partitions, each generation with a strictly newer
/// timestamp and a distinguishable value — the OVERLAPPING fixture (see the header).
pub(in crate::storage::sstable) async fn flush_overlapping_generations(
    temp_dir: &TempDir,
) -> PathBuf {
    let root = temp_dir.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        with_engine(&root, |engine, rt| {
            for generation in 0..GENERATIONS {
                for id in 0..PARTITIONS_PER_GENERATION {
                    engine
                        .write(create_test_mutation(
                            id,
                            &format!("gen{generation}-row-{id}"),
                            // Strictly increasing, so the LWW winner is
                            // unambiguous and the newest generation is the one a
                            // reconciled read shows.
                            1_000 + generation as i64,
                        ))
                        .expect("write");
                }
                rt.block_on(engine.flush())
                    .expect("flush")
                    .expect("flush wrote an SSTable");
            }
        })
    })
    .await
    .expect("fixture build task")
}

/// Build a write engine + its own current-thread runtime under `root`, run `writes`
/// against them, and return the data directory an `SSTableManager` opens.
fn with_engine(
    root: &std::path::Path,
    writes: impl FnOnce(&mut WriteEngine, &tokio::runtime::Runtime),
) -> PathBuf {
    let data_dir = root.join("data");
    let config = WriteEngineConfig::new(data_dir.clone(), root.join("wal"), create_test_schema());
    let mut engine = WriteEngine::new(config).expect("write engine");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    writes(&mut engine, &rt);
    data_dir
}

/// Rows a RECONCILED read of the overlapping fixture returns: last-write-wins
/// collapses every generation's copy of a partition into one row.
pub(in crate::storage::sstable) const fn reconciled_rows() -> usize {
    PARTITIONS_PER_GENERATION as usize
}

/// Rows the NON-reconciling token-order concat returns over the same fixture: every
/// generation's copy of every partition, superseded ones included.
pub(in crate::storage::sstable) const fn unreconciled_rows() -> usize {
    (GENERATIONS * PARTITIONS_PER_GENERATION) as usize
}

/// The value prefix only the NEWEST generation's cells carry. Every row of a
/// reconciled read must have it; a concatenated read also surfaces older prefixes.
pub(in crate::storage::sstable) fn newest_value_prefix() -> String {
    format!("gen{}-", GENERATIONS - 1)
}

pub(in crate::storage::sstable) async fn open_manager(
    data_dir: &std::path::Path,
) -> SSTableManager {
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

pub(in crate::storage::sstable) fn table_id() -> TableId {
    let schema = create_test_schema();
    TableId::from(format!("{}.{}", schema.keyspace, schema.table).as_str())
}

/// How a drained stream ended, plus how many rows it delivered.
pub(in crate::storage::sstable) struct Drained {
    pub(in crate::storage::sstable) rows: usize,
    /// `Some(message)` when the read terminated with an ERROR — whether the setup
    /// call itself failed or a stream item did; `None` on a clean end of stream.
    pub(in crate::storage::sstable) error: Option<String>,
}

/// Drain the per-row surface (`SSTableManager::scan_stream`) with NO schema.
///
/// `schema = None` on purpose: with a schema present and `write-support` on, a
/// multi-generation read routes to the authoritative `KWayMerger`
/// (`stream_generations_for_read`), NOT the lazy fan-out merge. Each reader still
/// resolves its own schema for decoding, so the rows are real.
pub(in crate::storage::sstable) async fn drain_per_row(manager: &SSTableManager) -> Drained {
    match manager
        .scan_stream(&table_id(), None, None, None, BUFFER)
        .await
    {
        Ok(mut stream) => {
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
        Err(e) => Drained {
            rows: 0,
            error: Some(e.to_string()),
        },
    }
}

/// Drain the batched surface (`SSTableManager::scan_stream_batched`), whose
/// multi-generation arm is the per-row → batch re-chunker.
pub(in crate::storage::sstable) async fn drain_batched(manager: &SSTableManager) -> Drained {
    match manager
        .scan_stream_batched(&table_id(), None, None, None, BUFFER)
        .await
    {
        Ok(mut stream) => {
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
        Err(e) => Drained {
            rows: 0,
            error: Some(e.to_string()),
        },
    }
}

/// Open the per-row surface WITH a schema — the RECONCILING route
/// (`stream_generations_for_read`), whose setup outcome the #3154 suite inspects by
/// TYPE. Kept separate from [`drain_reconciled`] so a caller can assert on the
/// typed setup `Err` instead of a stringified one.
pub(in crate::storage::sstable) async fn open_reconciling_stream(
    manager: &SSTableManager,
) -> crate::Result<crate::storage::sstable::reader::RowScanStream> {
    let schema = create_test_schema();
    manager
        .scan_stream(&table_id(), None, None, Some(&schema), BUFFER)
        .await
}

/// Drain the RECONCILING per-row surface, capturing every row's `name` value.
///
/// The captured values are what let a caller tell a reconciled result set from a
/// concatenated one, which a row COUNT alone cannot do once a fallback returns a
/// full-length answer.
pub(in crate::storage::sstable) async fn drain_reconciled(
    manager: &SSTableManager,
) -> (Drained, Vec<String>) {
    match open_reconciling_stream(manager).await {
        Ok(stream) => drain_stream(stream).await,
        Err(e) => (
            Drained {
                rows: 0,
                error: Some(e.to_string()),
            },
            Vec::new(),
        ),
    }
}

/// Drain an already-open reconciling stream, capturing every row's `name` value.
pub(in crate::storage::sstable) async fn drain_stream(
    mut stream: crate::storage::sstable::reader::RowScanStream,
) -> (Drained, Vec<String>) {
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

/// The CONTROL arm both suites run BEFORE injecting anything: the healthy read of
/// the OVERLAPPING fixture ends cleanly, is RECONCILED (one row per partition) and
/// shows only the newest generation's values.
///
/// Asserted in ONE place because it is a PRECONDITION, not a property: without it,
/// "the faulted read is not the concat's answer" is vacuous — a fixture that yields
/// nothing, or one where the two answers coincide, would pass too.
pub(in crate::storage::sstable) async fn assert_reconciled_control(manager: &SSTableManager) {
    assert_ne!(
        reconciled_rows(),
        unreconciled_rows(),
        "test precondition: with reconciled == unreconciled no test on this fixture \
         could tell a reconciled read from a silent concat fallback"
    );
    let (control, values) = drain_reconciled(manager).await;
    assert_eq!(
        control.error, None,
        "a healthy multi-generation reconciling read must end CLEANLY — no \
         fail-closed guard may turn a live producer into an error"
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
        values.len() == reconciled_rows() && values.iter().all(|v| v.starts_with(&newest)),
        "every reconciled row must carry the newest generation's ({newest}) value — \
         otherwise the control is not the LWW winner set and 'did not fall back' \
         proves nothing, got: {values:?}"
    );
}
