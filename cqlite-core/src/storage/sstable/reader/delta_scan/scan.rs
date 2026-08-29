//! Streaming driver for the delta-scan API (Issue #698 `scan_delta`).
//!
//! [`scan_delta`] opens one SSTable generation (a single `Data.db` file) and
//! streams [`DeltaRecord`](super::DeltaRecord)s in on-disk partition/clustering
//! order via a [`tokio::sync::mpsc`] channel.  It lives on the reader layer and
//! does **not** route through the query engine (which merges generations and
//! suppresses tombstones — the opposite contract).

use crate::storage::sstable::reader::SSTableReader;
use crate::types::{ColumnId, TombstoneType, Value};

use super::model::{CellDelta, CellMeta, DeltaRecord, RangeBound, RowKeys, ScanSummaryHandle};

// ---------------------------------------------------------------------------
// scan_delta — streaming API (Issue #698)
// ---------------------------------------------------------------------------

/// Return type of [`scan_delta`]: a channel receiver for [`DeltaRecord`]s and
/// a [`ScanSummaryHandle`] for collecting aggregate scan statistics.
pub type ScanDeltaOutput = (
    tokio::sync::mpsc::Receiver<crate::Result<DeltaRecord>>,
    ScanSummaryHandle,
);

/// Stream [`DeltaRecord`]s from a single SSTable generation directory.
///
/// Opens the first `Data.db` file found under `sstable_dir` and streams
/// [`DeltaRecord::Upsert`] and [`DeltaRecord::StaticUpsert`] records in
/// on-disk partition/clustering order via the returned channel receiver.
///
/// ## Contract
///
/// - Records stream in SSTable order (partition, then clustering).
/// - No cross-SSTable merge, no GC-grace filtering.
/// - Every live cell carries `writetime` and `expires_at`; TTL is never
///   resolved at scan time (idempotent output).
/// - Columns absent from a row are absent from `cells` (not null).
/// - A cell tombstone (`DELETE col FROM …`) appears as
///   `CellDelta { value: None, writetime: t, … }`.
/// - INSERT rows carry `liveness: Some(CellMeta { writetime, expires_at })`;
///   UPDATE rows carry `liveness: None`.
///
/// ## Memory
///
/// The channel is bounded by `buffer_size` records.  The parse task pauses
/// when the consumer falls behind.  Individual [`DeltaRecord`]s are bounded in
/// size and never accumulated into a full-table collection.
///
/// **Caveat**: `prepare_delta_scan` → `stitch_all_chunks` fully materialises the
/// decompressed data section of the SSTable into a single `Vec<u8>` before
/// streaming begins.  For very large SSTables this may approach the 128 MB
/// memory budget; callers should be aware that the stitched buffer is resident
/// for the duration of the scan.
///
/// ## Errors
///
/// A hard parse error (corrupt SSTable, missing schema, etc.) is forwarded
/// as an `Err(…)` item in the channel stream and terminates the scan, and is
/// counted ONCE into `cqlite.errors.total{category, subsystem="reader"}`
/// (issue #1704) — by [`drive_delta_scan`], except for an `SSTableReader::open`
/// failure, which [`SSTableReader::open`] counts itself. Counting is a pure side
/// effect: the `Err` the consumer receives is unchanged.
///
/// [`scan_delta`] returns immediately with an error if the SSTable directory
/// does not exist or contains no `Data.db` file.
pub fn scan_delta(
    sstable_dir: std::path::PathBuf,
    schema: crate::schema::TableSchema,
    buffer_size: usize,
) -> ScanDeltaOutput {
    let (tx, rx) = tokio::sync::mpsc::channel(buffer_size.max(1));
    let summary = ScanSummaryHandle::new();
    let summary_for_task = summary.clone();
    tokio::spawn(async move {
        if let Err(e) = drive_delta_scan(sstable_dir, schema, tx.clone(), summary_for_task).await {
            let _ = tx.send(Err(e)).await;
        }
    });
    (rx, summary)
}

/// Which side of the delta scan's error-counting seam a SETUP failure fell on
/// (issue #1704).
///
/// Setup is not uniform: one of its steps counts its own failures and the rest do
/// not, so "count everything at the seam" and "count nothing at the seam" are both
/// wrong. Naming the two cases keeps the choice at the call site that knows the
/// answer, instead of leaving it to be re-derived (wrongly) later.
enum DeltaOpenFailure {
    /// Raised by a step with NO instrumentation of its own (locating `Data.db`,
    /// platform init). The delta seam is authoritative for it and must count it.
    Uncounted(crate::Error),
    /// Raised by [`SSTableReader::open`], which records its OWN failure into
    /// `cqlite.errors.total{subsystem="reader"}`. Must NOT be counted again.
    CountedByOpen(crate::Error),
}

/// Setup for one delta scan: locate the generation's `Data.db`, build a platform,
/// open the reader.
///
/// Split out of [`run_scan_delta`] so the ONE self-instrumenting step sits outside
/// the counted region (issue #1704) — see [`drive_delta_scan`].
async fn open_delta_scan_reader(
    sstable_dir: &std::path::Path,
) -> std::result::Result<std::sync::Arc<SSTableReader>, DeltaOpenFailure> {
    let data_db = find_data_db(sstable_dir).map_err(DeltaOpenFailure::Uncounted)?;

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::Platform::new(&config).await.map_err(|e| {
        DeltaOpenFailure::Uncounted(crate::Error::corruption(format!(
            "scan_delta: platform init failed: {e}"
        )))
    })?);

    SSTableReader::open(&data_db, &config, platform)
        .await
        .map(std::sync::Arc::new)
        .map_err(|e| {
            // The open error is propagated VERBATIM rather than rewrapped as
            // `Error::corruption(..)` (issue #1704). The rewrap relabelled every
            // cause as corruption, so a plain unreadable/absent file reached the
            // caller as `corruption` while `SSTableReader::open`'s own increment
            // carried the true category (e.g. `io`) — the delivered error and the
            // metric disagreed about the same failure. The path context the rewrap
            // added is kept here, where it belongs, as a log field.
            tracing::error!(
                data_db = ?data_db,
                error = %e,
                "scan_delta: failed to open the generation's Data.db"
            );
            DeltaOpenFailure::CountedByOpen(e)
        })
}

/// [`scan_delta`]'s driver plus its error-counting seam (issue #1704).
///
/// # Why the open is OUTSIDE the counted region
///
/// [`SSTableReader::open`] SELF-INSTRUMENTS: its `Err` arm already calls
/// `record_error(e, "reader")` (`reader/mod.rs`), and that is the AUTHORITATIVE
/// boundary for an open failure because it is the innermost place that sees the
/// real error — its increment therefore carries the classifier's answer for the
/// ACTUAL cause. Counting again out here would report ONE failed open TWICE.
///
/// Every other step — locating `Data.db`, platform init, and the whole
/// stitch/parse/emit body — has no instrumentation of its own, so this seam is
/// authoritative for them, and it is the only place they are counted.
async fn drive_delta_scan(
    sstable_dir: std::path::PathBuf,
    schema: crate::schema::TableSchema,
    tx: tokio::sync::mpsc::Sender<crate::Result<DeltaRecord>>,
    summary: ScanSummaryHandle,
) -> crate::Result<()> {
    let reader = match open_delta_scan_reader(&sstable_dir).await {
        Ok(reader) => reader,
        Err(DeltaOpenFailure::CountedByOpen(e)) => return Err(e),
        Err(DeltaOpenFailure::Uncounted(e)) => {
            crate::observability::record_error(&e, "reader");
            return Err(e);
        }
    };
    // `record_result` counts on the `Err` arm and returns the result UNCHANGED — the
    // category comes from the classifier (`Error::obs_category`), never from here.
    crate::observability::record_result("reader", run_scan_delta(reader, schema, tx, summary).await)
}

/// Internal async driver for [`scan_delta`], over an ALREADY-OPEN reader.
async fn run_scan_delta(
    reader: std::sync::Arc<SSTableReader>,
    schema: crate::schema::TableSchema,
    tx: tokio::sync::mpsc::Sender<crate::Result<DeltaRecord>>,
    summary: ScanSummaryHandle,
) -> crate::Result<()> {
    // Wrap schema in Arc once — both the emit closure and parse call share the
    // same allocation rather than cloning the struct twice.
    let schema_arc = std::sync::Arc::new(schema);

    // Stitch + parse using a private per-scan cursor (issue #815): no scan-wide
    // mutex needed because the cursor owns its own file position / chunk index.
    // The parsing itself is synchronous and moves into spawn_blocking.
    let (stitched, parser) = reader.prepare_delta_scan().await?;

    let schema_for_parse = std::sync::Arc::clone(&schema_arc);
    let reader_arc = std::sync::Arc::clone(&reader);

    // The parse closure is synchronous; run it on a blocking thread so it can
    // `blocking_send` without stalling the runtime. Delta-scan still materializes
    // via `prepare_delta_scan`; streaming SELECT moved to a windowed driver (#1143).
    let parse_result = tokio::task::spawn_blocking(move || -> crate::Result<()> {
        parser.parse_block_emit_delta(
            &stitched,
            Some(&schema_for_parse),
            &reader_arc,
            |(
                partition_key_raw,
                cells,
                cell_meta,
                row_liveness_ts,
                is_static,
                is_row_tombstone,
                marked_for_delete_at,
                range_info,       // Issue #699: Some((start_vals,start_incl,end_vals,end_incl,del_at)) for range tombstone
                is_partition_tombstone, // Issue #699: true for partition-level tombstone
                col_complex_meta, // Issue #700 DS4: per-column ComplexColumnMeta
                liveness_expires_at_micros, // Issue #702: TTL liveness expiry in µs (epoch-s * 1_000_000)
            )| {
                // ----------------------------------------------------------------
                // Decode partition key from raw bytes.
                // (Needed for all record types, so decode upfront.)
                // ----------------------------------------------------------------
                let pk_columns = crate::storage::partition_key_codec::decode_partition_key_columns(
                    &partition_key_raw.0,
                    &schema_arc,
                )
                .map_err(|e| crate::Error::corruption(format!(
                    "scan_delta: failed to decode partition key (raw bytes {:?}): {e}",
                    partition_key_raw.0
                )))?;
                let partition_values: Vec<Value> = pk_columns.into_iter().map(|(_, v)| v).collect();

                // ----------------------------------------------------------------
                // Issue #699: Partition tombstone
                // ----------------------------------------------------------------
                if is_partition_tombstone {
                    let deleted_at = marked_for_delete_at.ok_or_else(|| {
                        crate::Error::corruption(format!(
                            "scan_delta: partition tombstone for pk={:?} has no markedForDeleteAt \
                             — cannot represent faithfully (no-heuristics, issue #28)",
                            partition_values
                        ))
                    })?;
                    let record = DeltaRecord::PartitionDelete {
                        partition_key: RowKeys::partition_only(partition_values),
                        deleted_at,
                    };
                    return match tx.blocking_send(Ok(record)) {
                        Ok(()) => Ok(std::ops::ControlFlow::Continue(())),
                        Err(_) => Ok(std::ops::ControlFlow::Break(())),
                    };
                }

                // ----------------------------------------------------------------
                // Issue #699: Range tombstone
                // ----------------------------------------------------------------
                if let Some((start_vals, start_incl, end_vals, end_incl, del_at)) = range_info {
                    let record = DeltaRecord::RangeDelete {
                        partition_key: RowKeys::partition_only(partition_values),
                        start: RangeBound::new(start_vals, start_incl),
                        end: RangeBound::new(end_vals, end_incl),
                        deleted_at: del_at,
                    };
                    return match tx.blocking_send(Ok(record)) {
                        Ok(()) => Ok(std::ops::ControlFlow::Continue(())),
                        Err(_) => Ok(std::ops::ControlFlow::Break(())),
                    };
                }

                // Build key-column name sets for filtering.
                let pk_col_names: std::collections::HashSet<&str> = schema_arc
                    .partition_keys.iter().map(|k| k.name.as_str()).collect();
                let clustering_col_names: std::collections::HashSet<&str> = schema_arc
                    .clustering_keys.iter().map(|ck| ck.name.as_str()).collect();
                let static_col_names: std::collections::HashSet<&str> = schema_arc
                    .columns.iter().filter(|c| c.is_static).map(|c| c.name.as_str()).collect();

                // Extract clustering values in declaration order.
                let clustering_values: Vec<Value> = schema_arc
                    .clustering_keys.iter()
                    .filter_map(|ck| cells.get(&ck.name).cloned())
                    .collect();

                // ----------------------------------------------------------------
                // Issue #699: Row tombstone
                // ----------------------------------------------------------------
                if is_row_tombstone {
                    let deleted_at = marked_for_delete_at.ok_or_else(|| {
                        crate::Error::corruption(format!(
                            "scan_delta: row tombstone for pk={:?} ck={:?} has no markedForDeleteAt \
                             — cannot represent faithfully (no-heuristics, issue #28)",
                            partition_values, clustering_values
                        ))
                    })?;
                    let record = DeltaRecord::RowDelete {
                        keys: RowKeys::new(partition_values, clustering_values),
                        deleted_at,
                    };
                    return match tx.blocking_send(Ok(record)) {
                        Ok(()) => Ok(std::ops::ControlFlow::Continue(())),
                        Err(_) => Ok(std::ops::ControlFlow::Break(())),
                    };
                }

                // ----------------------------------------------------------------
                // DS4 (Issue #700): Process element tombstone counts from this row's
                // collection columns and update the scan summary.
                // ----------------------------------------------------------------
                {
                    let mut row_element_tombstones: u64 = 0;
                    for (col_name, ccm) in &col_complex_meta {
                        if ccm.element_tombstone_count > 0 {
                            row_element_tombstones += ccm.element_tombstone_count;
                            tracing::warn!(
                                "scan_delta DS4: collection column '{}' has {} element-level tombstone(s) \
                                 that cannot be represented in v1 delta semantics (Issue #493 follow-up). \
                                 These removals are counted in the scan summary but not in the emitted records.",
                                col_name, ccm.element_tombstone_count
                            );
                        }
                    }
                    if row_element_tombstones > 0 {
                        summary.add_element_tombstones(row_element_tombstones);
                    }
                }

                // ----------------------------------------------------------------
                // Build CellDelta entries for non-key columns only.
                // ----------------------------------------------------------------
                let mut cell_deltas: Vec<(ColumnId, CellDelta)> = Vec::new();

                for (col_name, value) in &cells {
                    // Skip key columns — they are part of RowKeys, not cell payload.
                    if pk_col_names.contains(col_name.as_str())
                        || clustering_col_names.contains(col_name.as_str())
                    {
                        continue;
                    }
                    // Static upsert rows: only static columns.
                    // Regular upsert rows: only non-static columns.
                    if is_static && !static_col_names.contains(col_name.as_str()) {
                        continue;
                    }
                    if !is_static && static_col_names.contains(col_name.as_str()) {
                        continue;
                    }

                    let meta = cell_meta.get(col_name.as_str());
                    let (writetime, expires_at) = match meta {
                        Some(m) => {
                            let exp = m.expiration.as_ref().map(|e| {
                                // expires_at_seconds is epoch-seconds; delta-scan
                                // contract requires epoch-microseconds.
                                e.expires_at_seconds.saturating_mul(1_000_000)
                            });
                            (m.write_timestamp_micros, exp)
                        }
                        // Fallback: row-level liveness timestamp (should not happen
                        // when want_cell_metadata=true, but be defensive).
                        None => (row_liveness_ts.unwrap_or(0), None),
                    };

                    // DS4 (Issue #700): For collection columns, check whether this
                    // generation carries a collection-level tombstone (overwrite semantics).
                    // `replaced = true` signals downstream consumers to replace rather than
                    // merge the prior collection state.  Always `false` for scalar columns.
                    //
                    // Also: the normal read-path `cell_meta` stores `row_ts` for collection
                    // columns (not per-element max) to keep WRITETIME(col) semantics correct.
                    // For the delta-scan path we override writetime with the max element
                    // writetime from ComplexColumnMeta when it is non-zero (roborev Finding 1).
                    let (replaced, writetime) = match col_complex_meta.get(col_name.as_str()) {
                        Some(ccm) => {
                            let effective_wt = if ccm.max_element_writetime != 0 {
                                ccm.max_element_writetime
                            } else {
                                writetime
                            };
                            (ccm.has_collection_tombstone, effective_wt)
                        }
                        None => (false, writetime),
                    };

                    let cell = match value {
                        // Cell tombstone: IS_DELETED flag was set on the cell.
                        Value::Tombstone(info)
                            if info.tombstone_type == TombstoneType::CellTombstone =>
                        {
                            CellDelta {
                                value: None,
                                // Use the tombstone's own deletion_time (authoritative),
                                // NOT the cell_meta write_timestamp (which inherits row ts).
                                writetime: info.deletion_time,
                                expires_at: None,
                                replaced: false,
                            }
                        }
                        _ => CellDelta {
                            value: Some(value.clone()),
                            writetime,
                            expires_at,
                            replaced,
                        },
                    };

                    cell_deltas.push((ColumnId::new(col_name), cell));
                }

                // ----------------------------------------------------------------
                // Emit the appropriate DeltaRecord variant (Upsert / StaticUpsert).
                // ----------------------------------------------------------------
                let record = if is_static {
                    DeltaRecord::StaticUpsert {
                        partition_key: RowKeys::partition_only(partition_values),
                        cells: cell_deltas,
                    }
                } else {
                    // Build liveness CellMeta, carrying the TTL expiry when present (Issue #702).
                    let liveness = row_liveness_ts.map(|ts| {
                        match liveness_expires_at_micros {
                            Some(exp) => CellMeta::with_ttl(ts, exp),
                            None => CellMeta::new(ts),
                        }
                    });
                    DeltaRecord::Upsert {
                        keys: RowKeys::new(partition_values, clustering_values),
                        liveness,
                        cells: cell_deltas,
                    }
                };

                // Forward to the channel.  `blocking_send` is correct here
                // because we are inside spawn_blocking (not an async context).
                match tx.blocking_send(Ok(record)) {
                    Ok(()) => Ok(std::ops::ControlFlow::Continue(())),
                    Err(_) => {
                        // Consumer dropped: stop streaming.
                        Ok(std::ops::ControlFlow::Break(()))
                    }
                }
            },
        )
    })
    .await;

    match parse_result {
        Ok(result) => result,
        Err(join_err) => Err(crate::Error::corruption(format!(
            "scan_delta: parse task panicked: {join_err}"
        ))),
    }
}

/// Find the `Data.db` file inside an SSTable directory.
///
/// Cassandra names Data.db files with a generation prefix, e.g.:
/// `nb-1-big-Data.db` or `na-1-big-Data.db`.
///
/// Returns an error if no matching file is found.  If more than one `*-Data.db`
/// file is present (which violates the single-generation contract), a warning is
/// logged and the lexicographically smallest file name is returned so behaviour
/// is at least deterministic rather than OS-dependent.
fn find_data_db(dir: &std::path::Path) -> crate::Result<std::path::PathBuf> {
    if !dir.exists() {
        return Err(crate::Error::corruption(format!(
            "scan_delta: SSTable directory does not exist: {:?}",
            dir
        )));
    }

    let entries = std::fs::read_dir(dir).map_err(|e| {
        crate::Error::corruption(format!("scan_delta: cannot read directory {:?}: {e}", dir))
    })?;

    let mut candidates: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.ends_with("-Data.db"))
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();

    if candidates.is_empty() {
        return Err(crate::Error::corruption(format!(
            "scan_delta: no Data.db file found in {:?}",
            dir
        )));
    }

    if candidates.len() > 1 {
        candidates.sort();
        tracing::warn!(
            "scan_delta: {:?} contains {} Data.db files (expected 1 per generation); \
             using lexicographically first: {:?}. Consider compacting before scanning.",
            dir,
            candidates.len(),
            candidates[0]
        );
    }

    Ok(candidates.remove(0))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// The `scan_delta` corpus + parse tests. Split into two sibling `*_tests.rs` files
// per the campsite rule (#1116/#1135): this source file carried a 1494-line inline
// `mod tests`, so it was 1922 lines against the ~800-line source target.
#[cfg(test)]
#[path = "scan_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "scan_tombstone_tests.rs"]
mod tombstone_tests;
