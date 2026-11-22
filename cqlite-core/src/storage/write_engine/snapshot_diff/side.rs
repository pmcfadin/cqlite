//! Per-side work: reconcile one partition of one snapshot through the traced merger (spec R1),
//! enumerate a snapshot's partition keys, and name the generation behind a read failure (spec R7).

use std::collections::BTreeMap;

use super::types::{DiffError, DiffOptions, DiffSide, Side};
use crate::schema::{TableSchema, UdtRegistry};
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::write_engine::merge::trace::{
    CellDecision, ProbeOutcome, RecordingSink, TombstoneKind, TombstoneRecord, TraceSink, Verdict,
};
use crate::storage::write_engine::merge::{
    build_traced_merger_with_registry, compute_gc_before, MergeStep,
};
use crate::{Error, Result};

/// One side's reconciliation trail for one partition.
#[derive(Debug, Default)]
pub(super) struct SideTrail {
    pub cells: Vec<CellDecision>,
    pub tombstones: Vec<TombstoneRecord>,
}

/// A [`RecordingSink`] that stops recording once the serialized trail would exceed the budget, so
/// one huge partition cannot blow the process's memory target. Overflow is reported, never
/// swallowed ([`Error::ResultTooLarge`]).
struct BoundedSink {
    inner: RecordingSink,
    budget: u64,
    estimated: u64,
    overflowed: bool,
}

impl BoundedSink {
    fn reserve<T: serde::Serialize>(&mut self, event: &T) -> bool {
        if self.overflowed {
            return false;
        }
        let bytes = serde_json::to_vec(event)
            .map(|encoded| encoded.len() as u64)
            .unwrap_or(u64::MAX);
        if self.estimated.saturating_add(bytes) > self.budget {
            self.overflowed = true;
            return false;
        }
        self.estimated = self.estimated.saturating_add(bytes);
        true
    }
}

impl TraceSink for BoundedSink {
    fn cell(&mut self, decision: CellDecision) {
        if self.reserve(&decision) {
            self.inner.cell(decision);
        }
    }

    fn tombstone(&mut self, tombstone: TombstoneRecord) {
        if self.reserve(&tombstone) {
            self.inner.tombstone(tombstone);
        }
    }

    fn generation_probe(&mut self, run_index: usize, outcome: ProbeOutcome) {
        self.inner.generation_probe(run_index, outcome);
    }
}

/// Reconcile `key` across ONE side's generations only, in full-compaction configuration at the
/// pinned `now` (R1): the sequence `cqlite explain` runs — `build_traced_merger_with_registry`,
/// then `with_now_secs` / `with_gc_before_secs(compute_gc_before(..))` / `with_purge_safe(true)`,
/// `step()` to `Complete`, `into_trace_sink()`.
///
/// `schema` is the side's effective schema (`effective_compaction_schema` over its generations).
pub(super) fn trace_partition(
    input: &DiffSide,
    key: &[u8],
    schema: &TableSchema,
    udt_registry: Option<&UdtRegistry>,
    options: &DiffOptions,
    trail_budget: u64,
) -> Result<SideTrail> {
    let sink = BoundedSink {
        inner: RecordingSink::new(),
        budget: trail_budget,
        estimated: 0,
        overflowed: false,
    };
    let merger = build_traced_merger_with_registry(
        input.generations.clone(),
        &[key.to_vec()],
        schema,
        udt_registry,
        ScanCancel::default(),
        sink,
    )?;
    let Some(merger) = merger else {
        // Every generation reported the key absent: this side holds nothing for it.
        return Ok(SideTrail::default());
    };
    let mut merger = merger
        .with_now_secs(Some(options.now_secs))
        .with_gc_before_secs(compute_gc_before(schema, options.now_secs))
        .with_purge_safe(true);
    loop {
        match merger.step()? {
            MergeStep::Complete => break,
            MergeStep::Partition { .. } => {}
        }
    }
    let sink = merger.into_trace_sink();
    if sink.overflowed {
        return Err(Error::ResultTooLarge {
            budget_bytes: usize::try_from(sink.budget).unwrap_or(usize::MAX),
            estimated_bytes: usize::try_from(sink.estimated).unwrap_or(usize::MAX),
            rows: sink.inner.cells().len(),
        });
    }
    let (cells, mut tombstones, _probes) = sink.inner.into_parts();
    tombstones.extend(expiry_tombstones(
        &cells,
        compute_gc_before(schema, options.now_secs),
    ));
    Ok(SideTrail { cells, tombstones })
}

/// The tombstone Cassandra turns each expired TTL cell into, as a [`TombstoneRecord`] the shadow
/// and resurrection rules can see.
///
/// A cell that is not live at `now` is "equivalent to a tombstone": `AbstractCell.purge`
/// (cassandra-5.0.8 `db/rows/AbstractCell.java:78-97`) replaces an expired, not-yet-purgeable
/// expiring cell with `BufferCell.tombstone(column, timestamp(), localDeletionTime() - ttl(), path())`
/// — the SAME write timestamp, and a local deletion time of the cell's creation instant
/// (`expiry - ttl`, `:95`). That tombstone is what repair propagates, and it is purgeable under the
/// ordinary strict `localDeletionTime < gcBefore` rule (`PurgeFunction.java:41-42`), so its gc
/// phase is that of the creation time, not of the expiry. The merge already decided the cell
/// expired (`Verdict::Expired`, `reconcile.rs` Step 3b'); this only mirrors that decision, and only
/// for cells that survived reconciliation (the merge emits `Expired` for survivors alone).
fn expiry_tombstones(cells: &[CellDecision], gc_before: Option<i64>) -> Vec<TombstoneRecord> {
    cells
        .iter()
        .filter(|cell| cell.verdict == Verdict::Expired)
        .filter_map(|cell| {
            let (ttl, expires_at) = (i64::from(cell.ttl?), cell.expires_at?);
            let creation = expires_at.checked_sub(ttl)?;
            let ldt = u32::try_from(creation).ok()? as i32;
            Some(TombstoneRecord {
                kind: TombstoneKind::Cell,
                run_index: cell.run_index,
                clustering: cell.clustering.clone(),
                column: Some(cell.column.clone()),
                cell_path: cell.cell_path.clone(),
                deletion_time: cell.writetime,
                local_deletion_time: ldt,
                range_start: None,
                range_end: None,
                droppable_at_now: gc_before.is_some_and(|gc| creation < gc),
            })
        })
        .collect()
}

async fn open_generation(path: &std::path::Path) -> Result<SSTableReader> {
    use crate::config::DiskAccessMode;
    use crate::platform::Platform;
    use crate::Config;
    use std::sync::Arc;

    let mut config = Config::default();
    config.storage.use_mmap = false;
    config.storage.disk_access_mode = DiskAccessMode::Buffered;
    let platform = Arc::new(Platform::new(&config).await?);
    SSTableReader::open(path, &config, platform).await
}

/// Refuse a generation whose `Statistics.db` cannot be read strictly (spec R7).
///
/// The production reader is deliberately lenient about `Statistics.db`: a missing or damaged one
/// degrades to the caller's schema, which silently re-orders columns and re-bases every write
/// timestamp, and the merge still "succeeds" with a wrong answer — exactly the report this tool
/// must never emit. So every generation of both sides is pre-flighted once, up front, against
/// Cassandra's own layout and checksums (`metadata_check`) and then parsed (every Cassandra 5.0
/// format writes a `Statistics.db`).
pub(super) async fn preflight_generations(
    side: Side,
    input: &DiffSide,
) -> std::result::Result<(), DiffError> {
    use crate::config::DiskAccessMode;
    use crate::platform::Platform;
    use crate::storage::sstable::statistics_reader::StatisticsReader;
    use crate::Config;
    use std::sync::Arc;

    let mut config = Config::default();
    config.storage.use_mmap = false;
    config.storage.disk_access_mode = DiskAccessMode::Buffered;
    let platform = Arc::new(Platform::new(&config).await.map_err(DiffError::Core)?);
    for generation in &input.generations {
        let name = generation
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let Some(stem) = name.strip_suffix("Data.db") else {
            return Err(DiffError::SideRead {
                side,
                path: generation.display().to_string(),
                source: Error::InvalidInput("a generation path must name a *-Data.db file".into()),
            });
        };
        let statistics = generation.with_file_name(format!("{stem}Statistics.db"));
        let side_read = |source: Error| DiffError::SideRead {
            side,
            path: statistics.display().to_string(),
            source,
        };
        let bytes = std::fs::read(&statistics).map_err(|error| side_read(Error::Io(error)))?;
        super::metadata_check::validate_metadata_layout(&bytes)
            .map_err(|reason| side_read(Error::Corruption(format!("Statistics.db: {reason}"))))?;
        StatisticsReader::open(&statistics, platform.clone())
            .await
            .map_err(side_read)?;
    }
    Ok(())
}

/// Turn a failure from `side`'s merge into a [`DiffError::SideRead`] naming the generation: each
/// generation is re-opened in order and the first that cannot be opened is named; when all open,
/// the failure came from reading data and every generation of the side is named. Only ever runs
/// on the error path.
pub(super) async fn locate_failure(side: Side, input: &DiffSide, error: Error) -> DiffError {
    if matches!(error, Error::ResultTooLarge { .. } | Error::Cancelled) {
        return DiffError::Core(error);
    }
    for generation in &input.generations {
        if let Err(open_error) = open_generation(generation).await {
            return DiffError::SideRead {
                side,
                path: generation.display().to_string(),
                source: open_error,
            };
        }
    }
    DiffError::SideRead {
        side,
        path: input
            .generations
            .iter()
            .map(|generation| generation.display().to_string())
            .collect::<Vec<_>>()
            .join(", "),
        source: error,
    }
}

/// Add every distinct raw partition key held by `input` (the union over its generations) to
/// `into`, keyed by `(token, raw key)` so iteration is in token order. The walk is the structural, decompression-transparent
/// Data.db partition walk `rebuild` uses (`SSTableReader::distinct_partition_keys_with_positions`),
/// never a byte-pattern scan, and runs read-only.
pub(super) async fn enumerate_keys(
    side: Side,
    input: &DiffSide,
    schema: &TableSchema,
    into: &mut BTreeMap<(i64, Vec<u8>), ()>,
) -> std::result::Result<(), DiffError> {
    for generation in &input.generations {
        let reader = open_generation(generation)
            .await
            .map_err(|source| DiffError::SideRead {
                side,
                path: generation.display().to_string(),
                source,
            })?;
        let entries = reader
            .distinct_partition_keys_with_positions(Some(schema))
            .await
            .map_err(|source| DiffError::SideRead {
                side,
                path: generation.display().to_string(),
                source,
            })?;
        for (_, key) in entries {
            let token = crate::util::cassandra_murmur3::cassandra_murmur3_token(&key);
            into.insert((token, key), ());
        }
    }
    Ok(())
}
