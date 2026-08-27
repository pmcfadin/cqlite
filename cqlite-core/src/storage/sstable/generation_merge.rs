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
//!
//! NOTE (#1116 file-size): this file was over the 800-line target on `origin/main`.
//! Touching it for issue #3124 split two responsibilities out into child modules —
//! the streaming path's setup-outcome type (`merge_stream_setup`) and the
//! read-visibility filter's unit pins (`read_shadow_tests`) — which brings it back
//! under the target.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(not(feature = "tombstones"))]
use tokio::sync::{mpsc, oneshot};

use super::reader::parsing::row_decoder::now_clock::now_epoch_secs;
use super::reader::parsing::row_decoder::partition_shadow::{
    merged_row_shadowed_by_partition, PartitionShadow,
};
#[cfg(not(feature = "tombstones"))]
use super::stream_merge_probe;
use super::{reader, scan_merge};
use crate::storage::write_engine::merge::{CellData, KWayMerger, MergeEntry, MergeStep, RowData};
use crate::types::{CellWriteMetadata, TableId as CqlTableId};
use crate::{Result, RowCells, RowKey, ScanRow, Value};

#[cfg(not(feature = "tombstones"))]
mod merge_stream_setup;
#[cfg(not(feature = "tombstones"))]
pub(super) use merge_stream_setup::MergeStreamSetupError;

/// One reconciled metadata row inside the merge task, before per-cell metadata is
/// attached: `(partition key bytes, ScanRow row carrier, [(column,
/// write_timestamp_micros)])`.
type MergedMetaRow = (Vec<u8>, ScanRow, Vec<(String, i64)>);

/// Post-merge read-time visibility for the multi-generation read path (issue #1849).
///
/// A `SELECT` over a table directory with more than one SSTable generation routes
/// through the [`KWayMerger`], which performs cross-generation last-write-wins +
/// tombstone RECONCILIATION but NOT read-time TTL expiry / partition-deletion
/// visibility. Historically that let a multi-gen read return TTL-expired (or
/// partition-shadowed) cells as live, even after #1741 fixed the single-gen path.
///
/// This runs the merger's already-reconciled output through the SAME single-gen
/// [`PartitionShadow`] per-cell decision (`cell_shadowed_or_expired`) and row-level
/// partition-shadow decision (`merged_row_shadowed_by_partition`), so there is ONE
/// read-visibility implementation across single- and multi-generation reads. It adds
/// READ visibility only — reconciliation and the compaction/write path are untouched
/// (AC6). Range-tombstone shadowing of covered older cells is already applied by the
/// merger (#933); read-time cell/row TTL expiry is the gap this closes.
struct ReadShadow {
    /// Read-time TTL clock (epoch seconds), captured ONCE per scan so a scan crossing
    /// an expiration-second boundary decides every row with the same `now` — matching
    /// the single-gen parser's per-scan clock capture (issue #1849 AC4). Honours the
    /// `CQLITE_TTL_NOW_OVERRIDE_SECS` debug test seam via [`now_epoch_secs`].
    now_secs: i64,
    /// Names of the primary-key (partition + clustering) columns, surfaced by the
    /// compaction reader as pseudo-cells. Excluded from the row-level DATA-cell
    /// timestamp aggregate so only real data cells drive the partition-shadow row
    /// decision — matching the single-gen fold, which ignores pk/ck pseudo-cells.
    key_columns: HashSet<String>,
}

impl ReadShadow {
    fn new(schema: &crate::schema::TableSchema, now_secs: i64) -> Self {
        let mut key_columns =
            HashSet::with_capacity(schema.partition_keys.len() + schema.clustering_keys.len());
        for k in &schema.partition_keys {
            key_columns.insert(k.name.clone());
        }
        for k in &schema.clustering_keys {
            key_columns.insert(k.name.clone());
        }
        Self {
            now_secs,
            key_columns,
        }
    }

    /// Filter one merged `RowData::Live` row's cells for read visibility given the
    /// partition-tombstone `cover` (`markedForDeleteAt` µs, or `None`) and the row's
    /// primary-key liveness marker timestamp `marker_ts`
    /// (`MergeEntry::row_liveness.marker_timestamp`, #2374/#2789 — `None` when the row
    /// has no marker, or the merger already dropped the one the partition floor
    /// covered; a marker at/below `cover` can still arrive, and
    /// [`merged_row_shadowed_by_partition`] compares it rather than assuming it newer).
    /// Returns `None` when the WHOLE row is hidden (partition-shadowed), else the
    /// surviving cells with cell tombstones plus TTL-expired / partition-shadowed data
    /// cells dropped.
    fn filter_live(
        &self,
        cover: Option<i64>,
        marker_ts: Option<i64>,
        cells: Vec<CellData>,
    ) -> Option<Vec<CellData>> {
        let now = self.now_secs;
        let mut kept = Vec::with_capacity(cells.len());
        // Fold over REAL data cells (kept AND dropped) so a partition tombstone that
        // shadows every data cell is recognised at the row level — mirrors the
        // single-gen `agg_max_cell_ts` fold in `row_data.rs`.
        let mut max_data_ts: Option<i64> = None;
        // Issue #3094: mere PRESENCE of a merged cell TOMBSTONE — the multi-gen twin of
        // the single-gen `agg_has_deleted_cell`. It carries NO timestamp (one could only
        // RAISE the row max, i.e. UN-hide the row); it exists solely to defeat the
        // `i64::MIN` fail-safe in `PartitionShadow::has_shadow_evidence`.
        let mut has_deleted_data_cell = false;
        for cell in cells {
            // A cell tombstone is never live data (a deleted column is absent). Record
            // it as shadow evidence for DATA columns only, mirroring `row_data.rs`'s
            // aggregate, which never sees a pk/ck pseudo-cell (#3094).
            if matches!(cell.value, Value::Tombstone(_)) {
                has_deleted_data_cell |= !self.key_columns.contains(&cell.column);
                continue;
            }
            // Primary-key (partition + clustering) pseudo-cells are STRUCTURAL: the
            // compaction reader surfaces clustering columns as simple cells and both
            // the single-gen read loop and the merger's `apply_partition_shadowing`
            // leave them untouched (never subjected to the read-time shadow/expiry
            // drop). Keep them verbatim and out of the `max_data_ts` fold, so a row
            // kept alive by a newer DATA cell under a partition tombstone never loses
            // its clustering-key value (a pseudo-cell whose own `ts <= cover` must NOT
            // be stripped) — otherwise the multi-gen path would emit a malformed row
            // diverging from the single-gen path (issue #1849; roborev multi-gen finding).
            if self.key_columns.contains(&cell.column) {
                kept.push(cell);
                continue;
            }
            let eff_ts = Some(cell.timestamp);
            let eff_exp = cell_expiry_secs(&cell);
            let dropped = PartitionShadow::cell_shadowed_or_expired(cover, now, eff_ts, eff_exp);
            if let Some(t) = eff_ts {
                max_data_ts = Some(max_data_ts.map_or(t, |m| m.max(t)));
            }
            if !dropped {
                kept.push(cell);
            }
        }
        if merged_row_shadowed_by_partition(cover, marker_ts, max_data_ts, has_deleted_data_cell) {
            return None;
        }
        Some(kept)
    }
}

/// The read-time expiry instant (epoch seconds) of a merged cell, or `None` when it
/// is not an expiring cell. An expiring cell's `localDeletionTime` is its
/// `localExpirationTime`; it is reinterpreted UNSIGNED so a post-2038 expiry stored
/// as a negative `i32` bit pattern (oa/da `hasUIntDeletionTime`) is not wrapped
/// negative and wrongly treated as long-expired — matching the single-gen unsigned
/// LDT handling in `row_data.rs` (issue #1849 AC4).
fn cell_expiry_secs(cell: &CellData) -> Option<i64> {
    cell.ttl?;
    cell.local_deletion_time.map(|s| (s as u32) as i64)
}

/// The partition-level `markedForDeleteAt` (µs) carried by the synthetic
/// partition-tombstone carrier entry of a merged partition (issue #1072), or `None`
/// when the partition has no tombstone. Used as the read-side partition-shadow cover
/// across the partition's rows (issue #1849).
fn partition_cover(rows: &[MergeEntry]) -> Option<i64> {
    rows.iter()
        .find_map(|e| e.partition_deletion.map(|(mfda, _ldt)| mfda))
}

/// Convert one reconciled partition's merge rows into the live scan rows the read
/// path emits: drop cell tombstones, drop row tombstones, drop rows left empty.
///
/// The single emission kernel shared by the materializing plain merge and the
/// streaming driver so tombstone filtering can never drift between them (issue
/// #1334: emit the interned-name `ScanRow` carrier the read path consumes).
fn partition_live_rows(
    row_key: &RowKey,
    rows: Vec<MergeEntry>,
    shadow: &ReadShadow,
) -> Vec<(RowKey, ScanRow)> {
    // Issue #1849: partition-tombstone cover for this partition's rows, applied as a
    // read-side shadow floor alongside per-cell read-time TTL expiry (post-merge).
    let cover = partition_cover(&rows);
    let mut out = Vec::new();
    for entry in rows {
        // Read BEFORE `row_data` is moved out of `entry` below (`RowLiveness` is
        // `Copy`): the surviving marker's write ts, #2374/#2789.
        let marker_ts = entry.row_liveness.marker_timestamp;
        match entry.row_data {
            RowData::Live { cells } => {
                // Read-time visibility: drop cell tombstones + TTL-expired /
                // partition-shadowed data cells; `None` hides the whole row.
                let Some(surviving) = shadow.filter_live(cover, marker_ts, cells) else {
                    continue;
                };
                let row_cells: RowCells = surviving
                    .into_iter()
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
    // Issue #2063: one operation-level scan-admission permit; rationale + cancellation
    // shape in `scan_admission.rs` `# Scope`. Moved into the closure below.
    let admission = reader::scan_stream_windowed::scan_admission::admit().await;

    // Own the bounds/target so the merge body can use them without borrowing
    // across the await; cheap clone of the key bytes.
    let start_key = start_key.cloned();
    let end_key = end_key.cloned();
    let target_key = target_key.map(|k| k.as_bytes().to_vec());

    let paths = ordered_generation_paths(reader_list);
    let schema = schema.clone();

    let mut merged = tokio::task::spawn_blocking(move || -> Result<Vec<(RowKey, ScanRow)>> {
        let _admission = admission; // #2063: hold across the detached blocking work.
                                    // Issue #1849: capture the read-time TTL clock ONCE per scan.
        let shadow = ReadShadow::new(&schema, now_epoch_secs());
        let mut merger = KWayMerger::new(paths, &schema)?;
        let mut out = Vec::new();
        while let MergeStep::Partition { key, rows } = merger.step()? {
            let row_key = RowKey::new(key.key.clone());

            // Partition-targeted point read (#1579): keep ONLY the target and stop.
            if let Some(ref target) = target_key {
                if row_key.as_bytes() != target.as_slice() {
                    continue;
                }
                out.extend(partition_live_rows(&row_key, rows, &shadow));
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
            out.extend(partition_live_rows(&row_key, rows, &shadow));
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

/// Partition-SEEKING sibling of [`merge_generations_for_read`] for the
/// multi-candidate `WHERE pk = ?` point read (issue #2096).
///
/// Where [`merge_generations_for_read`] with a `target_key` still builds the
/// FULL-SCAN `KWayMerger::new` and sequentially DECODES every partition with token
/// <= the target before breaking (O(partitions-below-target)), this reuses the
/// Flight point path's partition-SEEKING merger
/// ([`build_single_partition_merger_from_readers`](crate::storage::write_engine::build_single_partition_merger_from_readers),
/// #2207/#2346): each candidate seeks straight to the target partition's `Data.db`
/// offset (BTI trie / `Index.db`), or fail-safe filter-scans one SSTable when its
/// index is unavailable, then reconciles through the SAME `KWayMerger`
/// (`from_row_iterators`, run_index = position). The read-visibility kernel is
/// IDENTICAL to the materializing helper — the same [`ReadShadow`] (#1849) captured
/// ONCE, the same [`partition_live_rows`] emission + token-order guard, and
/// candidates ordered NEWEST→OLDEST like [`ordered_generation_paths`] — so the
/// output is byte-for-byte `merge_generations_for_read(.., Some(target))`, only
/// over O(target) work. Blocking builder + `step()` run on `spawn_blocking`.
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
pub(super) async fn seek_merge_generations_for_read(
    candidates: &[Arc<reader::SSTableReader>],
    schema: &crate::schema::TableSchema,
    target_key: &RowKey,
) -> Result<Vec<(RowKey, ScanRow)>> {
    use crate::storage::scan_cancel::ScanCancel;
    use crate::storage::write_engine::merge::{
        build_single_partition_merger_from_readers, PointAccessRecording,
    };

    // Issue #2063: one operation-level scan-admission permit; ONLY call site is the
    // top-level `scan_partition_clustering`, never nested. Rationale + cancellation
    // shape in `scan_admission.rs` `# Scope`.
    let admission = reader::scan_stream_windowed::scan_admission::admit().await;

    // NEWEST→OLDEST like `ordered_generation_paths`, so the seeking merger's
    // run_index (= position) equals the full-scan merger's LWW tie-break rank.
    let mut ordered: Vec<Arc<reader::SSTableReader>> = candidates.to_vec();
    ordered.sort_by_key(|b| std::cmp::Reverse(b.generation));

    let schema = schema.clone();
    let target_bytes = target_key.as_bytes().to_vec();

    let mut merged = tokio::task::spawn_blocking(move || -> Result<Vec<(RowKey, ScanRow)>> {
        let _admission = admission; // #2063: hold across the detached blocking work.
                                    // Issue #1849: same read-visibility kernel `merge_generations_for_read`
                                    // applies (read-time TTL clock captured ONCE), REQUIRED for byte-identity.
        let shadow = ReadShadow::new(&schema, now_epoch_secs());
        let keys = [target_bytes.clone()];
        let Some(mut merger) = build_single_partition_merger_from_readers(
            ordered,
            &keys,
            &schema,
            ScanCancel::new(),
            // The executor records this logical access at its own storage
            // boundary (`StorageEngine::scan_partition_clustering`), so
            // recording here as well would count one read twice (#2827).
            PointAccessRecording::CallerRecords,
        )?
        else {
            return Ok(Vec::new()); // no candidate holds the target
        };
        let mut out = Vec::new();
        while let MergeStep::Partition { key, rows } = merger.step()? {
            let row_key = RowKey::new(key.key.clone());
            // A fail-safe filter-scan run could surface a prefix-collision key; keep
            // only the exact target and stop once seen (partition keys are unique).
            if row_key.as_bytes() != target_bytes.as_slice() {
                continue;
            }
            out.extend(partition_live_rows(&row_key, rows, &shadow));
            break;
        }
        Ok(out)
    })
    .await
    .map_err(|e| {
        crate::Error::Storage(format!("seeking cross-generation read merge task: {e}"))
    })??;

    // Parity guard with the materializing helper: stable TOKEN-order sort (no-op for
    // a single partition, kept for byte-parity, issue #1580).
    scan_merge::sort_by_token_order(&mut merged, None, |(k, _)| k);
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
    // Issue #2063: one operation-level scan-admission permit. Held as an OUTER future
    // guard across the async per-reader `scan_with_cell_metadata` loop (cancellation
    // there is clean — no detached blocking work exists yet, so early release is
    // harmless), THEN MOVED into the `spawn_blocking` merge closure below so the permit
    // is held until the detached blocking work TERMINATES. This matches the plain/seek
    // helpers: no phase both runs detached blocking work AND has released the permit.
    let admission = reader::scan_stream_windowed::scan_admission::admit().await;

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
                    .entry((row_key.as_bytes().to_vec(), column))
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
        let _admission = admission; // #2063: hold across the detached blocking work.
                                    // Issue #1849: capture the read-time TTL clock ONCE per scan.
        let shadow = ReadShadow::new(&merge_schema, now_epoch_secs());
        let mut merger = KWayMerger::new(paths, &merge_schema)?;
        let mut out = Vec::new();
        while let MergeStep::Partition { key, rows } = merger.step()? {
            let row_key = RowKey::new(key.key.clone());

            // Partition-targeted point read (#1579): keep ONLY the target and stop.
            if let Some(ref target) = target_for_merge {
                if row_key.as_bytes() != target.as_slice() {
                    continue;
                }
                push_metadata_rows(&key.key, rows, &mut out, &shadow);
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
            push_metadata_rows(&key.key, rows, &mut out, &shadow);
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
        results.push((RowKey::new(key_bytes), value, meta_map));
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
fn push_metadata_rows(
    key_bytes: &[u8],
    rows: Vec<MergeEntry>,
    out: &mut Vec<MergedMetaRow>,
    shadow: &ReadShadow,
) {
    // Issue #1849: same read-side partition cover + per-cell TTL/shadow filter as the
    // plain path, so the WRITETIME/TTL projection agrees with `SELECT *`.
    let cover = partition_cover(&rows);
    for entry in rows {
        // Read BEFORE `row_data` is moved out of `entry` (`RowLiveness` is `Copy`).
        let marker_ts = entry.row_liveness.marker_timestamp;
        if let RowData::Live { cells } = entry.row_data {
            // `filter_live` drops cell tombstones + TTL-expired / partition-shadowed
            // data cells; `None` hides the whole row (partition-shadowed).
            let Some(surviving) = shadow.filter_live(cover, marker_ts, cells) else {
                continue;
            };
            let mut row_cells: RowCells = Vec::with_capacity(surviving.len());
            let mut timestamps: Vec<(String, i64)> = Vec::with_capacity(surviving.len());
            for c in surviving {
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
/// Construction (`KWayMerger::new`, which SPAWNS one producer thread per input —
/// each input's `SSTableReader::open`, and so every format/version gate, runs inside
/// that thread, not here) happens on the blocking task and is signalled back over a
/// oneshot BEFORE any streaming — and the ways that can fail are kept apart by the
/// returned [`MergeStreamSetupError`] rather than flattened into one `Error`. Only a
/// merger-INELIGIBLE input (an unsupported format/version) is `fallback_eligible`,
/// and because of that thread boundary NO production construction failure is ever
/// classified that way — the reachable ones are `Error::Schema` and `Error::Storage`,
/// and both now propagate (see [`MergeStreamSetupError`]'s module doc for the full
/// enumeration and why that arm is kept as a defensive one). A producer that DIED
/// without signalling — joined here to recover its panic — propagates too, because
/// the caller must not answer it with the non-reconciling concat (issues
/// #3124/#3154, roborev).
///
/// A `step()` error mid-stream is delivered as an `Err` item on the channel, and a
/// task that dies mid-stream is caught by the returned [`reader::RowScanStream`]'s
/// join (issues #3106/#3124).
///
/// Deliberate, documented error-path asymmetry (issue #1579): the caller's
/// fallback-to-concatenation applies ONLY to the construction failure above (nothing
/// has been streamed, so falling back cannot mix reconciled and unreconciled rows). A
/// `step()` failure after partitions were already emitted is NEVER retried — the
/// `Err` item ends the stream, giving the caller an honest cutoff instead of a
/// silently half-reconciled table (the materializing `merge_generations_for_read` has
/// no equivalent mid-collection signal; it propagates the whole call as `Err`).
#[cfg(not(feature = "tombstones"))]
pub(super) async fn stream_generations_for_read(
    reader_list: &[Arc<reader::SSTableReader>],
    schema: &crate::schema::TableSchema,
    start_key: Option<&RowKey>,
    end_key: Option<&RowKey>,
    buffer_size: usize,
) -> std::result::Result<reader::RowScanStream, MergeStreamSetupError> {
    let start_key = start_key.cloned();
    let end_key = end_key.cloned();
    let paths = ordered_generation_paths(reader_list);
    let schema = schema.clone();

    // Issue #3124 (site 5): this task's ONE test-only fault checkpoint needs the
    // reader identity INSIDE the task, so capture it as an owned scope (a zero-sized
    // no-op in a production build). Any generation's path identifies the table
    // directory a test scopes to.
    let fault_scope = crate::storage::producer_fault::FaultScope::capture(|| {
        paths.first().cloned().unwrap_or_default()
    });

    let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
    let (out_tx, out_rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(buffer_size.max(1));

    let task = tokio::task::spawn_blocking(move || {
        // Inside the construction window, i.e. BEFORE either readiness arm runs, so
        // an armed fault reproduces exactly "the producer died without signalling".
        fault_scope.checkpoint(crate::storage::producer_fault::ScanTaskSite::CrossGenerationMerge);
        // Issue #1849: capture the read-time TTL clock ONCE per scan.
        let shadow = ReadShadow::new(&schema, now_epoch_secs());
        // Issue #3154: a test may make construction REPORT a chosen error variant, so
        // the narrowed fallback classification below can be proven per class (I/O vs
        // corruption vs merger-ineligible). `None` — and `KWayMerger::new` called
        // exactly as before — in every production build, where
        // `injected_construction_error` returns `None` without touching a registry.
        let constructed = match fault_scope.injected_construction_error() {
            Some(injected) => Err(injected),
            None => KWayMerger::new(paths, &schema),
        };
        let mut merger = match constructed {
            Ok(m) => {
                // Signal readiness; if the caller already dropped, stop.
                if ready_tx.send(Ok(())).is_err() {
                    return;
                }
                m
            }
            Err(e) => {
                // REPORT the failure verbatim; whether it earns the caller's fallback
                // to the lazy per-reader concat is classified from its VARIANT at the
                // receiving end (issue #3154), not decided here.
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

            let row_key = RowKey::new(key.key.clone());
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

            let live = partition_live_rows(&row_key, rows, &shadow);
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
        // The cross-generation reconciling merge is the top-level read operation
        // (issue #1701), measured FORMAT-AGNOSTICALLY: its rows are reconciled across
        // possibly mixed BIG/BTI inputs, so no single format label is honest here.
        Ok(Ok(())) => Ok(reader::RowScanStream::new_measured(out_rx, task, None)),
        // A REPORTED construction failure is CLASSIFIED from its `Error` variant (issue
        // #3154): only a merger-INELIGIBLE input earns the caller's concat fallback, and
        // an I/O / corruption / other runtime failure propagates. Answering the latter
        // with the concat returned a full-length UNRECONCILED result set under `Ok`.
        Ok(Err(e)) => Err(MergeStreamSetupError::from_construction_failure(e)),
        // `ready_tx` is dropped-WITHOUT-send on exactly one condition: the blocking
        // task unwound before either readiness arm ran. So this `Err` ⟺ a dead
        // producer — JOIN the retained handle to recover the real cause (the panic
        // message) instead of dropping it unjoined, and report it as the
        // fallback-INELIGIBLE variant (issue #3124, roborev).
        Err(_) => Err(MergeStreamSetupError::ProducerDied(
            merge_stream_setup::dead_merge_producer_error(task.await),
        )),
    }
}

/// The merger expects inputs ordered newest → oldest (run_index 0 = newest) for
/// its stable tie-break; the reader `Vec` order is discovery-dependent, so sort
/// explicitly by generation descending and collect the input `Data.db` paths.
fn ordered_generation_paths(reader_list: &[Arc<reader::SSTableReader>]) -> Vec<PathBuf> {
    let mut ordered: Vec<&Arc<reader::SSTableReader>> = reader_list.iter().collect();
    ordered.sort_by_key(|b| std::cmp::Reverse(b.generation));
    ordered.iter().map(|r| r.file_path()).collect()
}

#[cfg(test)]
mod read_shadow_tests;

// The multi-generation streaming read path's shared fixture + reconciled/unreconciled
// oracle, and the issue-#3154 end-to-end pins built on it. `not(tombstones)` because
// that build routes `scan_stream` through the materializing `scan`, so neither the
// streaming merge nor its setup-outcome type exists there.
#[cfg(all(test, not(feature = "tombstones")))]
pub(super) mod multi_gen_fixture;
#[cfg(all(test, not(feature = "tombstones")))]
mod setup_fail_closed_tests;
