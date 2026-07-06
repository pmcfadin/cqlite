//! Cross-generation read reconciliation for the SSTable manager (issues #883,
//! #885, #957, #1579).
//!
//! When a table directory holds more than one SSTable generation, plain
//! concatenation of each reader's live rows is wrong: the same
//! `(partition, clustering)` row can appear in several generations, and a
//! row/cell tombstone in a newer generation suppresses only its OWN generation's
//! copy — so concatenation duplicates overwritten rows and resurrects rows deleted
//! in a later generation. These helpers reconcile across generations with the
//! authoritative [`KWayMerger`](crate::storage::write_engine::KWayMerger) — the
//! same last-write-wins + tombstone-shadowing logic compaction uses — so the read
//! path returns Cassandra's merged, deduplicated, tombstone-honouring result.
//!
//! Three drivers, ONE reconciliation kernel (the merger's `step()`):
//!
//! - [`merge_generations_for_read`] — materializing plain read (`scan`,
//!   partition-targeted point reads). Collects the reconciled rows into a `Vec`
//!   in Cassandra token order.
//! - [`merge_generations_for_read_with_metadata`] — the `WRITETIME`/`TTL`
//!   projection sibling; additionally surfaces the winning cell's per-cell write
//!   metadata.
//! - [`stream_generations_for_read`] — the STREAMING plain read (`scan_stream`,
//!   issue #1579 / D3). Feeds each stepped partition straight into a bounded
//!   channel instead of collecting the whole table, so live heap is O(one
//!   partition + channel) and time-to-first-row is O(first partition), not
//!   O(full merge).
//!
//! All three yield partitions in the merger's `DecoratedKey` order = `(token,
//! key)`, which is byte-identical to [`scan_merge::sort_by_token_order`]. The
//! materializing paths still apply that stable sort as a no-op guard; the
//! streaming path relies on the merger's order directly (issue #1579).
//!
//! Extracted from `sstable/mod.rs` (issue #1116 campsite split): behaviour of the
//! two materializing helpers is unchanged apart from the new `target_key`
//! partition-targeting parameter (issue #1579, point-read path).
//!
//! The whole module is gated on `write-support` at its `mod` declaration in the
//! parent (`sstable/mod.rs`).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(not(feature = "tombstones"))]
use tokio::sync::{mpsc, oneshot};

#[cfg(not(feature = "tombstones"))]
use super::stream_merge_probe;
use super::{reader, scan_merge};
use crate::storage::write_engine::merge::{KWayMerger, MergeEntry, MergeStep, RowData};
use crate::types::{CellWriteMetadata, TableId as CqlTableId};
use crate::{Result, RowCells, RowKey, ScanRow, Value};

/// One reconciled metadata row inside the merge task, before per-cell metadata is
/// attached: `(partition key bytes, ScanRow row carrier, [(column,
/// write_timestamp_micros)])`.
type MergedMetaRow = (Vec<u8>, ScanRow, Vec<(String, i64)>);

/// Convert one reconciled partition's merge rows into the live scan rows the read
/// path emits: drop cell tombstones, drop row tombstones, drop rows left empty.
///
/// The single emission kernel shared by the materializing plain merge and the
/// streaming driver so tombstone filtering can never drift between them (issue
/// #1334: emit the interned-name `ScanRow` carrier the read path consumes).
fn partition_live_rows(row_key: &RowKey, rows: Vec<MergeEntry>) -> Vec<(RowKey, ScanRow)> {
    let mut out = Vec::new();
    for entry in rows {
        match entry.row_data {
            RowData::Live { cells } => {
                let row_cells: RowCells = cells
                    .into_iter()
                    .filter(|c| !matches!(c.value, Value::Tombstone(_)))
                    .map(|c| (Arc::from(c.column.as_str()), c.value))
                    .collect();
                if !row_cells.is_empty() {
                    out.push((row_key.clone(), ScanRow::Row(row_cells)));
                }
            }
            // Row tombstone: the row is deleted across all generations — suppress.
            RowData::Tombstone { .. } => {}
        }
    }
    out
}

/// Reconcile multiple SSTable generations into the single authoritative live-row
/// set, returning them materialized in Cassandra token order (issue #883).
///
/// This drives the same [`KWayMerger`](crate::storage::write_engine::KWayMerger)
/// the compaction path uses, so reconciliation is byte-for-byte the
/// last-write-wins + tombstone-shadowing logic (`merge_partition_rows`): per-cell
/// LWW by write timestamp, row/cell tombstones shadow older cells, and
/// fully-deleted rows are dropped. The merger manages its own reader
/// threads/runtimes internally, so it runs on a blocking task.
///
/// `start_key`/`end_key` bound the merged output to the same inclusive
/// `[start_key, end_key]` key range the per-reader
/// [`scan`](reader::SSTableReader::scan) applies (skip `key < start`, skip
/// `key > end`, using `RowKey`'s `Ord`) — Issue #957. The range filter runs
/// before `limit`, matching the per-reader scan order (range then limit).
///
/// `target_key` restricts the output to a SINGLE partition (the point-read path,
/// issue #1579): keep only the partition whose raw key bytes equal `target_key`
/// and STOP as soon as it is found. Partition keys are unique in the merger's
/// output, so once the target is seen nothing later can match — this avoids
/// converting every other partition to `ScanRow` and decoding past the target,
/// while remaining byte-identical to the caller's former
/// `retain(|r| r.key == partition_key)`. `target_key` and the range bounds are
/// not combined by any current caller (point reads pass `None`/`None` bounds).
///
/// With `None` target and `None`/`None` bounds the output is byte-for-byte the
/// full reconciled set.
pub(super) async fn merge_generations_for_read(
    reader_list: &[Arc<reader::SSTableReader>],
    schema: &crate::schema::TableSchema,
    start_key: Option<&RowKey>,
    end_key: Option<&RowKey>,
    limit: Option<usize>,
    target_key: Option<&RowKey>,
) -> Result<Vec<(RowKey, ScanRow)>> {
    // Own the bounds/target so the merge body can use them without borrowing
    // across the await; cheap clone of the key bytes.
    let start_key = start_key.cloned();
    let end_key = end_key.cloned();
    let target_key = target_key.map(|k| k.as_bytes().to_vec());

    let paths = ordered_generation_paths(reader_list);
    let schema = schema.clone();

    let mut merged = tokio::task::spawn_blocking(move || -> Result<Vec<(RowKey, ScanRow)>> {
        let mut merger = KWayMerger::new(paths, &schema)?;
        let mut out = Vec::new();
        while let MergeStep::Partition { key, rows } = merger.step()? {
            let row_key = RowKey(key.key.clone());

            // Partition-targeted point read (#1579): keep ONLY the target and stop.
            if let Some(ref target) = target_key {
                if row_key.as_bytes() != target.as_slice() {
                    continue;
                }
                out.extend(partition_live_rows(&row_key, rows));
                break;
            }

            // Full / range scan: inclusive [start, end] on `RowKey` order (#957).
            if let Some(ref start) = start_key {
                if &row_key < start {
                    continue;
                }
            }
            if let Some(ref end) = end_key {
                if &row_key > end {
                    continue;
                }
            }
            out.extend(partition_live_rows(&row_key, rows));
        }
        Ok(out)
    })
    .await
    .map_err(|e| crate::Error::Storage(format!("cross-generation read merge task: {e}")))??;

    // Match the plain-scan contract: the merger already emits partitions in
    // Cassandra token order with clustering rows contiguous within a partition.
    // Preserve that with a stable TOKEN-order sort (issue #1580), then apply
    // LIMIT. The sort is a no-op ordering-wise when the input is already
    // token-ordered; it only guards against a stray divergence.
    scan_merge::sort_by_token_order(&mut merged, limit, |(k, _)| k);
    Ok(merged)
}

/// Metadata-aware sibling of [`merge_generations_for_read`] for the
/// `WRITETIME(col)` / `TTL(col)` projection path (Issue #885).
///
/// Reconciles multiple SSTable generations with the same
/// [`KWayMerger`](crate::storage::write_engine::KWayMerger) (per-cell LWW +
/// row/cell tombstone shadowing) and additionally surfaces the **winning** cell's
/// per-cell write metadata:
///
/// - `write_timestamp_micros` comes straight from the winning `CellData`
///   (`reconcile_cluster` keeps each surviving cell's own timestamp), so it is the
///   WRITETIME of the cell that actually won cross-generation LWW.
/// - `expiration` (TTL) is recovered best-effort from the per-reader
///   `scan_with_cell_metadata` outputs: for each surviving `(key, column)` we take
///   the newest reader-surfaced metadata and attach its expiration only when its
///   timestamp matches the merge winner. Absent/mismatched ⇒ `None`.
///
/// `start_key`/`end_key`/`target_key`/`limit` behave exactly as in
/// [`merge_generations_for_read`], keeping this definitionally in lockstep with
/// the plain helper. When `target_key` is `Some`, the best-effort TTL scan is also
/// bounded to that single partition (issue #1579, point-read path).
pub(super) async fn merge_generations_for_read_with_metadata(
    reader_list: &[Arc<reader::SSTableReader>],
    schema: &crate::schema::TableSchema,
    start_key: Option<&RowKey>,
    end_key: Option<&RowKey>,
    limit: Option<usize>,
    target_key: Option<&RowKey>,
) -> Result<Vec<(RowKey, ScanRow, HashMap<String, CellWriteMetadata>)>> {
    // Own the bounds so the merge body can use them without borrowing across the
    // await; cheap clone of the key bytes. Mirrors the plain helper.
    let owned_start = start_key.cloned();
    let owned_end = end_key.cloned();
    let target_bytes = target_key.map(|k| k.as_bytes().to_vec());

    // Best-effort TTL source: gather each reader's own per-cell metadata and keep,
    // per (row-key bytes, column), the entry with the newest write timestamp. The
    // merger surfaces accurate WRITETIME but no TTL, so this recovers expiration
    // for the winning cell when the reader format carries it. For a point read the
    // scan is bounded to the target partition (#1579); otherwise to the read range.
    let table_id = CqlTableId::from(format!("{}.{}", schema.keyspace, schema.table).as_str());
    let mut ttl_lookup: HashMap<(Vec<u8>, String), CellWriteMetadata> = HashMap::new();
    for reader in reader_list {
        let (ttl_start, ttl_end): (Option<&RowKey>, Option<&RowKey>) = match target_key {
            Some(t) => (Some(t), Some(t)),
            None => (owned_start.as_ref(), owned_end.as_ref()),
        };
        let per_reader = reader
            .scan_with_cell_metadata(&table_id, ttl_start, ttl_end, None, Some(schema))
            .await?;
        for (row_key, _value, meta) in per_reader {
            for (column, cell_meta) in meta {
                ttl_lookup
                    .entry((row_key.0.clone(), column))
                    .and_modify(|existing| {
                        if cell_meta.write_timestamp_micros > existing.write_timestamp_micros {
                            *existing = cell_meta.clone();
                        }
                    })
                    .or_insert(cell_meta);
            }
        }
    }

    let paths = ordered_generation_paths(reader_list);
    let merge_schema = schema.clone();
    let start_key = owned_start;
    let end_key = owned_end;
    let target_for_merge = target_bytes;

    let merged_rows = tokio::task::spawn_blocking(move || -> Result<Vec<MergedMetaRow>> {
        let mut merger = KWayMerger::new(paths, &merge_schema)?;
        let mut out = Vec::new();
        while let MergeStep::Partition { key, rows } = merger.step()? {
            let row_key = RowKey(key.key.clone());

            // Partition-targeted point read (#1579): keep ONLY the target and stop.
            if let Some(ref target) = target_for_merge {
                if row_key.as_bytes() != target.as_slice() {
                    continue;
                }
                push_metadata_rows(&key.key, rows, &mut out);
                break;
            }

            // Full / range scan: inclusive [start, end] on `RowKey` order (#957).
            if let Some(ref start) = start_key {
                if &row_key < start {
                    continue;
                }
            }
            if let Some(ref end) = end_key {
                if &row_key > end {
                    continue;
                }
            }
            push_metadata_rows(&key.key, rows, &mut out);
        }
        Ok(out)
    })
    .await
    .map_err(|e| crate::Error::Storage(format!("cross-generation metadata merge task: {e}")))??;

    // Attach per-cell metadata: WRITETIME from the merge winner, TTL recovered from
    // the reader lookup only when its timestamp matches the winner.
    let mut results: Vec<(RowKey, ScanRow, HashMap<String, CellWriteMetadata>)> =
        Vec::with_capacity(merged_rows.len());
    for (key_bytes, value, timestamps) in merged_rows {
        let mut meta_map: HashMap<String, CellWriteMetadata> =
            HashMap::with_capacity(timestamps.len());
        for (column, write_ts) in timestamps {
            let expiration = ttl_lookup
                .get(&(key_bytes.clone(), column.clone()))
                .filter(|m| m.write_timestamp_micros == write_ts)
                .and_then(|m| m.expiration.clone());
            meta_map.insert(
                column,
                CellWriteMetadata {
                    write_timestamp_micros: write_ts,
                    expiration,
                },
            );
        }
        results.push((RowKey(key_bytes), value, meta_map));
    }

    // Stable TOKEN-order sort (issue #1580), then LIMIT — identical ordering to the
    // plain merge path; the metadata payload rides in the tuple's tail.
    scan_merge::sort_by_token_order(&mut results, limit, |(k, _, _)| k);
    Ok(results)
}

/// Convert one reconciled partition's merge rows into metadata-carrying rows: the
/// live cells plus each surviving cell's `(column, write_timestamp)`. Shared by the
/// range and point-targeted branches of the metadata merge so the tombstone
/// filtering + timestamp capture live once.
fn push_metadata_rows(key_bytes: &[u8], rows: Vec<MergeEntry>, out: &mut Vec<MergedMetaRow>) {
    for entry in rows {
        if let RowData::Live { cells } = entry.row_data {
            let mut row_cells: RowCells = Vec::with_capacity(cells.len());
            let mut timestamps: Vec<(String, i64)> = Vec::with_capacity(cells.len());
            for c in cells {
                // Drop cell tombstones: a deleted column is absent.
                if matches!(c.value, Value::Tombstone(_)) {
                    continue;
                }
                timestamps.push((c.column.clone(), c.timestamp));
                row_cells.push((Arc::from(c.column.as_str()), c.value));
            }
            if !row_cells.is_empty() {
                out.push((key_bytes.to_vec(), ScanRow::Row(row_cells), timestamps));
            }
        }
        // Row tombstones suppress the row entirely (no emission).
    }
}

/// STREAMING cross-generation reconciliation for `scan_stream` (issue #1579 / D3).
///
/// Runs the authoritative [`KWayMerger`](crate::storage::write_engine::KWayMerger)
/// on a blocking task and feeds each stepped partition's live rows STRAIGHT into a
/// bounded channel via `blocking_send` (backpressure preserved), instead of
/// collecting the entire reconciled table, sorting it, and dribbling it. Live heap
/// is therefore O(one partition + channel), and the first row is available after
/// the first partition rather than after the whole merge.
///
/// Reconciliation is byte-identical to [`merge_generations_for_read`] (shared
/// `partition_live_rows` kernel), and the emission order is the merger's
/// `DecoratedKey` = `(token, key)` order — byte-identical to the collect+sort
/// path's [`scan_merge::sort_by_token_order`] output (issue #1579 ordering
/// guardrail).
///
/// Construction (`KWayMerger::new`, which opens the input files) happens on the
/// blocking task; its success/failure is signalled back over a oneshot BEFORE any
/// streaming, so the caller can FALL BACK to the lazy per-reader streaming merge on
/// a construction error exactly as the materializing `scan` falls back to
/// concatenation. A runtime `step()` error mid-stream is delivered as an `Err`
/// item on the channel (the consumer sees it), matching the lazy path's read-error
/// behaviour.
///
/// This is a deliberate, documented error-path asymmetry (issue #1579): the
/// caller's fallback-to-concatenation only ever applies to the CONSTRUCTION
/// failure above (nothing has been streamed yet, so falling back cannot mix
/// reconciled and unreconciled rows). A `step()` failure after some partitions
/// were already emitted downstream is NEVER retried/fallen-back — it ends the
/// stream via the `Err` channel item instead. That is safer than it sounds: the
/// materializing `merge_generations_for_read` has no equivalent mid-collection
/// failure signal — a `step()` error there simply propagates the whole call as
/// `Err` before any partial `Vec` is returned to the caller. The streaming
/// driver's `Err` item preserves the same "no half-reconciled result surfaces
/// silently" guarantee while still letting the caller observe exactly how many
/// good rows it already received.
#[cfg(not(feature = "tombstones"))]
pub(super) async fn stream_generations_for_read(
    reader_list: &[Arc<reader::SSTableReader>],
    schema: &crate::schema::TableSchema,
    start_key: Option<&RowKey>,
    end_key: Option<&RowKey>,
    buffer_size: usize,
) -> Result<mpsc::Receiver<Result<(RowKey, ScanRow)>>> {
    let start_key = start_key.cloned();
    let end_key = end_key.cloned();
    let paths = ordered_generation_paths(reader_list);
    let schema = schema.clone();

    let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
    let (out_tx, out_rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(buffer_size.max(1));

    tokio::task::spawn_blocking(move || {
        let mut merger = match KWayMerger::new(paths, &schema) {
            Ok(m) => {
                // Signal readiness; if the caller already dropped, stop.
                if ready_tx.send(Ok(())).is_err() {
                    return;
                }
                m
            }
            Err(e) => {
                // Let the caller fall back to the lazy per-reader streaming merge.
                let _ = ready_tx.send(Err(e));
                return;
            }
        };

        loop {
            let step = match merger.step() {
                Ok(s) => s,
                Err(e) => {
                    let _ = out_tx.blocking_send(Err(e));
                    return;
                }
            };
            let (key, rows) = match step {
                MergeStep::Partition { key, rows } => (key, rows),
                MergeStep::Complete => return,
            };

            let row_key = RowKey(key.key.clone());
            if let Some(ref start) = start_key {
                if &row_key < start {
                    continue;
                }
            }
            if let Some(ref end) = end_key {
                if &row_key > end {
                    continue;
                }
            }

            let live = partition_live_rows(&row_key, rows);
            // Issue #1579: the streaming producer holds ONE partition's rows
            // resident at a time — record the window (not the whole table) so the
            // memory guard observes O(window).
            stream_merge_probe::record_resident(live.len() as u64);
            for entry in live {
                if out_tx.blocking_send(Ok(entry)).is_err() {
                    return; // consumer dropped
                }
            }
        }
    });

    match ready_rx.await {
        Ok(Ok(())) => Ok(out_rx),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(crate::Error::Storage(
            "cross-generation streaming merge task ended before signalling readiness".to_string(),
        )),
    }
}

/// The merger expects inputs ordered newest → oldest (run_index 0 = newest) for
/// its stable tie-break; the reader `Vec` order is discovery-dependent, so sort
/// explicitly by generation descending and collect the input `Data.db` paths.
fn ordered_generation_paths(reader_list: &[Arc<reader::SSTableReader>]) -> Vec<PathBuf> {
    let mut ordered: Vec<&Arc<reader::SSTableReader>> = reader_list.iter().collect();
    ordered.sort_by(|a, b| b.generation.cmp(&a.generation));
    ordered.iter().map(|r| r.file_path.clone()).collect()
}
