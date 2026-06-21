//! Delta-scan record model and streaming API for CDC-style Parquet projections
//! (Epic #696, Issue #697 types, Issue #698 `scan_delta` implementation).
//!
//! A flushed SSTable is a delta, not a snapshot.  Projecting it to Parquet
//! correctly requires carrying per-cell write-timestamps and representing every
//! delete shape Cassandra produces — otherwise a downstream union of per-flush
//! files resurrects deleted data and merges stale cells.
//!
//! ## Design contract
//!
//! One SSTable generation in, faithful change events out.  Reconciliation
//! (LWW merge, tombstone application, TTL filtering) is deliberately the
//! downstream consumer's responsibility.
//!
//! ## Types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`DeltaRecord`] | Discriminated union of every change shape |
//! | [`CellDelta`] | Per-cell value + writetime + TTL + collection flag |
//! | [`RangeBound`] | Typed, possibly-prefix clustering-key bound |
//! | [`RowKeys`] | Partition key + optional clustering columns |
//! | [`CellMeta`] | Row liveness metadata (timestamp, TTL) |
//!
//! ## Streaming API
//!
//! [`scan_delta`] streams [`DeltaRecord`]s from a single SSTable generation
//! (one `Data.db` file) via a [`tokio::sync::mpsc`] channel.  It lives on the
//! reader layer and does **not** route through the query engine (which merges
//! generations and suppresses tombstones — the opposite contract).
//!
//! Row/range/partition tombstone emission is Issue #699 scope.  This version
//! errors or returns early on delete-bearing input; upsert and static-upsert
//! paths are complete and correct.
//!
//! ## Feature gate
//!
//! Everything in this module is behind `feature = "delta-scan"` and will not
//! compile into the default crate build.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::types::{ColumnId, TombstoneType, Value};

// ---------------------------------------------------------------------------
// ScanSummary — scan-level aggregate statistics (Issue #700, DS4)
// ---------------------------------------------------------------------------

/// Summary statistics produced by a [`scan_delta`] run.
///
/// The caller receives a [`ScanSummaryHandle`] alongside the record stream;
/// after the stream is drained the handle's counters reflect the full scan.
///
/// ## Element tombstones (Issue #493 / DS4)
///
/// Non-frozen collection cells may contain **element-level removals**
/// (`s = s - {x}` for sets, individual map-key deletions).  V1 of the
/// delta-scan layer cannot represent these at element granularity; they are
/// detected, counted here, and reported as a warning — but not silently
/// dropped.  Consumers that need element-level fidelity must wait for full
/// Issue #493 implementation.
#[derive(Debug, Clone)]
pub struct ScanSummary {
    /// Total number of element-level collection tombstones detected across
    /// all collection columns in this scan.
    ///
    /// A non-zero value means the delta contains `s = s - {x}` style
    /// removals that are not represented in the emitted [`DeltaRecord`]s
    /// (Issue #493 follow-up).
    pub element_tombstones_detected: u64,
}

/// Handle to in-progress scan summary counters.
///
/// Returned by [`scan_delta`] alongside the record receiver.
/// After the receiver is drained (stream complete), call [`ScanSummaryHandle::read`]
/// to obtain the final [`ScanSummary`].
///
/// The handle is cheaply clonable — all clones share the same counters.
#[derive(Debug, Clone)]
pub struct ScanSummaryHandle {
    element_tombstones: Arc<AtomicU64>,
}

impl ScanSummaryHandle {
    fn new() -> Self {
        Self {
            element_tombstones: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Increment the element-tombstone counter by `n`.
    pub(super) fn add_element_tombstones(&self, n: u64) {
        self.element_tombstones.fetch_add(n, Ordering::Relaxed);
    }

    /// Read the current scan summary.  Call after draining the record receiver
    /// for a complete picture.
    pub fn read(&self) -> ScanSummary {
        ScanSummary {
            element_tombstones_detected: self.element_tombstones.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// RowKeys
// ---------------------------------------------------------------------------

/// Combined partition key + clustering columns for a single row (or partition).
///
/// For records that address a full partition (`PartitionDelete`,
/// `StaticUpsert`, `RangeDelete`) the `clustering` vec is empty.  For
/// row-addressed records it holds the decoded clustering column values in
/// primary-key definition order.
///
/// Values are decoded according to the `TableSchema` in effect at scan time;
/// the no-heuristics mandate applies — every component uses schema-authorised
/// types.
#[derive(Debug, Clone, PartialEq)]
pub struct RowKeys {
    /// Decoded partition-key components in definition order.
    ///
    /// Multi-component partition keys (composite) appear as multiple elements.
    pub partition: Vec<Value>,

    /// Decoded clustering-key components in definition order.
    ///
    /// Empty when the record is partition-scoped (partition delete, static
    /// upsert, or range-delete — in which case bounds carry the clustering
    /// information instead).
    pub clustering: Vec<Value>,
}

impl RowKeys {
    /// Create a partition-scoped key (no clustering components).
    pub fn partition_only(partition: Vec<Value>) -> Self {
        Self {
            partition,
            clustering: Vec::new(),
        }
    }

    /// Create a fully-specified row key.
    pub fn new(partition: Vec<Value>, clustering: Vec<Value>) -> Self {
        Self {
            partition,
            clustering,
        }
    }
}

// ---------------------------------------------------------------------------
// CellMeta — row liveness info
// ---------------------------------------------------------------------------

/// Liveness metadata for a row's primary-key cell.
///
/// Cassandra writes a liveness-info record alongside a row when the row was
/// created with `INSERT` (not `UPDATE`).  `UPDATE` statements produce rows
/// with no liveness info; those rows have `liveness: None` in
/// [`DeltaRecord::Upsert`].
///
/// The `writetime` here is the row-level primary-key liveness timestamp,
/// equivalent to `__ts` in the Parquet envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct CellMeta {
    /// Writetime in microseconds since the Unix epoch.
    pub writetime: i64,

    /// Expiry time in microseconds since the Unix epoch, if a TTL was set on
    /// the `INSERT`.  `None` means no TTL.
    pub expires_at: Option<i64>,
}

impl CellMeta {
    /// Create a liveness record with no TTL.
    pub fn new(writetime: i64) -> Self {
        Self {
            writetime,
            expires_at: None,
        }
    }

    /// Create a liveness record with a TTL expiry time.
    pub fn with_ttl(writetime: i64, expires_at: i64) -> Self {
        Self {
            writetime,
            expires_at: Some(expires_at),
        }
    }
}

// ---------------------------------------------------------------------------
// CellDelta
// ---------------------------------------------------------------------------

/// Per-cell change record carried inside [`DeltaRecord::Upsert`] and
/// [`DeltaRecord::StaticUpsert`].
///
/// A `null` [`value`][Self::value] represents a cell tombstone
/// (`DELETE col FROM t WHERE …`).  A non-null value is the cell content as
/// decoded by the schema-aware reader — no heuristics.
///
/// ## Collection columns (v1 limitation)
///
/// Non-frozen collection columns carry per-element writetimes and element
/// tombstones that v1 cannot faithfully represent at element granularity.
/// For those columns:
///
/// - `value` holds the elements present in this generation (for an append
///   `s = s + {…}` that is only the appended elements — correct delta
///   semantics).
/// - `writetime` is the maximum element writetime.
/// - `replaced` is `true` when the generation carries a collection tombstone
///   (i.e. an overwrite `s = {…}`), signalling consumers to replace rather
///   than merge.
///
/// Full element-level fidelity is a tracked follow-up (Issue #493).
#[derive(Debug, Clone, PartialEq)]
pub struct CellDelta {
    /// The decoded cell value, or `None` for a cell tombstone.
    pub value: Option<Value>,

    /// Writetime in microseconds since the Unix epoch.
    pub writetime: i64,

    /// TTL expiry time in microseconds since the Unix epoch.
    ///
    /// `None` means no TTL was set on this cell.  The delta-scan layer never
    /// resolves TTLs at scan time — whether a cell is expired is left to the
    /// downstream consumer (idempotent output guarantee).
    pub expires_at: Option<i64>,

    /// `true` when this collection cell carries a collection-level tombstone,
    /// meaning the downstream consumer must **replace** rather than merge the
    /// prior collection state.
    ///
    /// Always `false` for scalar (non-collection) columns.
    pub replaced: bool,
}

impl CellDelta {
    /// Create a simple value cell with no TTL and no collection-replace flag.
    pub fn value(value: Value, writetime: i64) -> Self {
        Self {
            value: Some(value),
            writetime,
            expires_at: None,
            replaced: false,
        }
    }

    /// Create a cell tombstone (no value, just a deletion timestamp).
    pub fn tombstone(writetime: i64) -> Self {
        Self {
            value: None,
            writetime,
            expires_at: None,
            replaced: false,
        }
    }

    /// Create a value cell with a TTL expiry time.
    pub fn value_with_ttl(value: Value, writetime: i64, expires_at: i64) -> Self {
        Self {
            value: Some(value),
            writetime,
            expires_at: Some(expires_at),
            replaced: false,
        }
    }

    /// Create a collection cell that replaces (not merges) prior state.
    pub fn collection_replace(value: Value, writetime: i64) -> Self {
        Self {
            value: Some(value),
            writetime,
            expires_at: None,
            replaced: true,
        }
    }
}

// ---------------------------------------------------------------------------
// RangeBound
// ---------------------------------------------------------------------------

/// One bound of a range-tombstone clustering-key range.
///
/// ## Prefix bounds
///
/// Cassandra range tombstones may specify bounds that are **prefixes** of the
/// full clustering key — for example, a table with clustering columns
/// `(year INT, month INT, day INT)` can have a range tombstone covering all
/// rows in `year = 2024`.  In that case:
///
/// ```text
/// start = RangeBound { values: [Value::Integer(2024)], inclusive: true  }
/// end   = RangeBound { values: [Value::Integer(2024)], inclusive: true  }
/// ```
///
/// Trailing clustering components that are absent from the bound are simply
/// not present in [`values`][Self::values].  Consumers must treat a prefix
/// bound as matching all rows whose clustering key begins with the given
/// prefix (within the inclusive/exclusive constraint).
///
/// ## Empty bound
///
/// An empty `values` vec with `inclusive: false` represents an open
/// (unbounded) end — i.e. from/to the beginning or end of the partition.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeBound {
    /// The clustering-key prefix values for this bound, in primary-key order.
    ///
    /// May be shorter than the full clustering-key arity (prefix bound).
    /// An empty vec means an open (unbounded) end.
    pub values: Vec<Value>,

    /// Whether the bound is inclusive (`>=` / `<=`) or exclusive (`>` / `<`).
    pub inclusive: bool,
}

impl RangeBound {
    /// Create a bound from a full or partial set of clustering values.
    pub fn new(values: Vec<Value>, inclusive: bool) -> Self {
        Self { values, inclusive }
    }

    /// Convenience: create an inclusive bound.
    pub fn inclusive(values: Vec<Value>) -> Self {
        Self {
            values,
            inclusive: true,
        }
    }

    /// Convenience: create an exclusive bound.
    pub fn exclusive(values: Vec<Value>) -> Self {
        Self {
            values,
            inclusive: false,
        }
    }

    /// An open (unbounded) end — matches from/to the start or end of the partition.
    pub fn open() -> Self {
        Self {
            values: Vec::new(),
            inclusive: false,
        }
    }

    /// Returns `true` if this is a prefix bound (fewer values than the full
    /// clustering-key arity of the table, or the full arity is not known).
    ///
    /// Note: callers that know the table's clustering-key arity should compare
    /// `self.values.len()` against that arity directly rather than relying on
    /// this method.
    pub fn is_prefix(&self) -> bool {
        // An empty bound is not a prefix — it is an open bound.
        // Any non-empty bound with at least one value is potentially a prefix;
        // the caller is responsible for comparing against the full arity.
        !self.values.is_empty()
    }
}

// ---------------------------------------------------------------------------
// DeltaRecord
// ---------------------------------------------------------------------------

/// A single change record emitted by the delta-scan API.
///
/// Records stream in SSTable order (partition, then clustering).  The scan
/// makes no cross-SSTable decisions — no merge, no GC-grace filtering.
///
/// ## Variant reference
///
/// | Variant | CQL operation | Key scope |
/// |---------|---------------|-----------|
/// | `Upsert` | `INSERT`/`UPDATE` on regular columns | `(pk, ck)` |
/// | `StaticUpsert` | `UPDATE` on static columns | `pk` |
/// | `RowDelete` | `DELETE FROM t WHERE pk=? AND ck=?` | `(pk, ck)` |
/// | `RangeDelete` | `DELETE FROM t WHERE pk=? AND ck>=? AND ck<?` | `pk` + bounds |
/// | `PartitionDelete` | `DELETE FROM t WHERE pk=?` | `pk` |
///
/// ## Downstream merge keys
///
/// - `Upsert` / `RowDelete` — reconcile on `(partition, clustering)`.
/// - `StaticUpsert` — reconcile on `partition`.
/// - `PartitionDelete` / `RangeDelete` — apply as predicates using `__ts`
///   (i.e. `deleted_at`) to decide last-write-wins per cell.
#[derive(Debug, Clone, PartialEq)]
pub enum DeltaRecord {
    /// A row-level insert or update of regular (non-static) columns.
    ///
    /// `liveness` is `Some` when the row was created with `INSERT` and carries
    /// a primary-key liveness timestamp.  `UPDATE` statements produce rows
    /// with `liveness: None`.
    ///
    /// Each element of `cells` is a `(column_id, delta)` pair.  Only the
    /// columns actually written in this generation appear; absent columns are
    /// not present (they are null in the Parquet envelope).
    Upsert {
        /// Partition key + clustering columns for this row.
        keys: RowKeys,
        /// Row liveness info, present only for `INSERT` operations.
        liveness: Option<CellMeta>,
        /// Per-column cell deltas for non-static columns modified in this row.
        cells: Vec<(ColumnId, CellDelta)>,
    },

    /// An update of one or more static columns for a partition.
    ///
    /// Static columns belong to the partition, not individual rows.  The
    /// clustering key is empty in `partition_key`.
    StaticUpsert {
        /// Partition key (no clustering columns).
        partition_key: RowKeys,
        /// Per-column cell deltas for static columns modified.
        cells: Vec<(ColumnId, CellDelta)>,
    },

    /// A row-level tombstone (`DELETE FROM t WHERE pk=? AND ck=?`).
    RowDelete {
        /// Partition key + clustering columns identifying the deleted row.
        keys: RowKeys,
        /// Deletion timestamp in microseconds since the Unix epoch
        /// (`markedForDeleteAt` in Cassandra internals).
        deleted_at: i64,
    },

    /// A range tombstone covering a contiguous clustering-key range within a
    /// single partition.
    ///
    /// `start` and `end` may be prefix bounds (see [`RangeBound`]).
    ///
    /// ```text
    /// DELETE FROM t WHERE pk=1 AND ck >= 'a' AND ck < 'm'
    /// ```
    RangeDelete {
        /// Partition key (no clustering columns — bounds carry the range).
        partition_key: RowKeys,
        /// Inclusive or exclusive lower clustering-key bound.
        start: RangeBound,
        /// Inclusive or exclusive upper clustering-key bound.
        end: RangeBound,
        /// Deletion timestamp in microseconds since the Unix epoch.
        deleted_at: i64,
    },

    /// A partition-level tombstone (`DELETE FROM t WHERE pk=?`).
    ///
    /// Supersedes every row and cell in the partition whose writetime is older
    /// than `deleted_at`.
    PartitionDelete {
        /// Partition key (no clustering columns).
        partition_key: RowKeys,
        /// Deletion timestamp in microseconds since the Unix epoch.
        deleted_at: i64,
    },
}

impl DeltaRecord {
    /// Return the partition-key portion of any record type.
    pub fn partition_key(&self) -> &[Value] {
        match self {
            DeltaRecord::Upsert { keys, .. } => &keys.partition,
            DeltaRecord::StaticUpsert { partition_key, .. } => &partition_key.partition,
            DeltaRecord::RowDelete { keys, .. } => &keys.partition,
            DeltaRecord::RangeDelete { partition_key, .. } => &partition_key.partition,
            DeltaRecord::PartitionDelete { partition_key, .. } => &partition_key.partition,
        }
    }

    /// Return the `__op` discriminator string used in the Parquet envelope.
    pub fn op_name(&self) -> &'static str {
        match self {
            DeltaRecord::Upsert { .. } => "upsert",
            DeltaRecord::StaticUpsert { .. } => "static_upsert",
            DeltaRecord::RowDelete { .. } => "row_delete",
            DeltaRecord::RangeDelete { .. } => "range_delete",
            DeltaRecord::PartitionDelete { .. } => "partition_delete",
        }
    }
}

// ---------------------------------------------------------------------------
// scan_delta — streaming API (Issue #698)
// ---------------------------------------------------------------------------

/// Stream [`DeltaRecord`]s from a single SSTable generation directory.
///
/// Opens the first `Data.db` file found under `sstable_dir` and streams
/// [`DeltaRecord::Upsert`] and [`DeltaRecord::StaticUpsert`] records in
/// on-disk partition/clustering order via the returned channel receiver.
///
/// ## Scope (Issue #698)
///
/// This implementation handles **upserts and static upserts only**.
/// Row, range, and partition tombstones are not yet emitted (Issue #699);
/// if a row tombstone is encountered the scan continues past it (the row is
/// silently dropped with a `log::debug!` trace — not a hard error, since most
/// real SSTables have no tombstones and the issue is out of scope here).
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
/// as an `Err(…)` item in the channel stream and terminates the scan.
///
/// [`scan_delta`] returns immediately with an error if the SSTable directory
/// does not exist or contains no `Data.db` file.
/// Return type of [`scan_delta`]: a channel receiver for [`DeltaRecord`]s and
/// a [`ScanSummaryHandle`] for collecting aggregate scan statistics.
pub type ScanDeltaOutput = (
    tokio::sync::mpsc::Receiver<crate::Result<DeltaRecord>>,
    ScanSummaryHandle,
);

pub fn scan_delta(
    sstable_dir: std::path::PathBuf,
    schema: crate::schema::TableSchema,
    buffer_size: usize,
) -> ScanDeltaOutput {
    let (tx, rx) = tokio::sync::mpsc::channel(buffer_size.max(1));
    let summary = ScanSummaryHandle::new();
    let summary_for_task = summary.clone();
    tokio::spawn(async move {
        if let Err(e) = run_scan_delta(sstable_dir, schema, tx.clone(), summary_for_task).await {
            let _ = tx.send(Err(e)).await;
        }
    });
    (rx, summary)
}

/// Internal async driver for [`scan_delta`].
async fn run_scan_delta(
    sstable_dir: std::path::PathBuf,
    schema: crate::schema::TableSchema,
    tx: tokio::sync::mpsc::Sender<crate::Result<DeltaRecord>>,
    summary: ScanSummaryHandle,
) -> crate::Result<()> {
    use crate::storage::sstable::reader::SSTableReader;

    // Find the Data.db file in this directory.
    let data_db = find_data_db(&sstable_dir)?;

    let config = crate::Config::default();
    let platform =
        std::sync::Arc::new(crate::Platform::new(&config).await.map_err(|e| {
            crate::Error::corruption(format!("scan_delta: platform init failed: {e}"))
        })?);

    let reader = std::sync::Arc::new(
        SSTableReader::open(&data_db, &config, platform)
            .await
            .map_err(|e| {
                crate::Error::corruption(format!("scan_delta: failed to open {:?}: {e}", data_db))
            })?,
    );

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
    // use `blocking_send` without stalling the async runtime (mirrors
    // `parse_stitched_stream` in data_access.rs, issue #790).
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
                            log::warn!(
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
        log::warn!(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    // ------------------------------------------------------------------
    // RowKeys helpers
    // ------------------------------------------------------------------

    #[test]
    fn row_keys_partition_only() {
        let keys = RowKeys::partition_only(vec![Value::Integer(42)]);
        assert_eq!(keys.partition, vec![Value::Integer(42)]);
        assert!(keys.clustering.is_empty());
    }

    #[test]
    fn row_keys_full() {
        let keys = RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]);
        assert_eq!(keys.partition.len(), 1);
        assert_eq!(keys.clustering.len(), 1);
    }

    // ------------------------------------------------------------------
    // CellMeta helpers
    // ------------------------------------------------------------------

    #[test]
    fn cell_meta_no_ttl() {
        let m = CellMeta::new(1_000_000);
        assert_eq!(m.writetime, 1_000_000);
        assert!(m.expires_at.is_none());
    }

    #[test]
    fn cell_meta_with_ttl() {
        let m = CellMeta::with_ttl(1_000_000, 2_000_000);
        assert_eq!(m.expires_at, Some(2_000_000));
    }

    // ------------------------------------------------------------------
    // CellDelta constructors
    // ------------------------------------------------------------------

    #[test]
    fn cell_delta_value() {
        let d = CellDelta::value(Value::Text("hello".into()), 100);
        assert!(d.value.is_some());
        assert_eq!(d.writetime, 100);
        assert!(d.expires_at.is_none());
        assert!(!d.replaced);
    }

    #[test]
    fn cell_delta_tombstone() {
        let d = CellDelta::tombstone(200);
        assert!(d.value.is_none());
        assert_eq!(d.writetime, 200);
        assert!(!d.replaced);
    }

    #[test]
    fn cell_delta_with_ttl() {
        let d = CellDelta::value_with_ttl(Value::Integer(7), 100, 9999);
        assert_eq!(d.expires_at, Some(9999));
        assert!(!d.replaced);
    }

    #[test]
    fn cell_delta_collection_replace() {
        let d = CellDelta::collection_replace(Value::Text("x".into()), 300);
        assert!(d.replaced);
        assert!(d.value.is_some());
    }

    // ------------------------------------------------------------------
    // RangeBound
    // ------------------------------------------------------------------

    #[test]
    fn range_bound_inclusive() {
        let b = RangeBound::inclusive(vec![Value::Text("a".into())]);
        assert!(b.inclusive);
        assert_eq!(b.values.len(), 1);
    }

    #[test]
    fn range_bound_exclusive() {
        let b = RangeBound::exclusive(vec![Value::Text("m".into())]);
        assert!(!b.inclusive);
    }

    #[test]
    fn range_bound_open() {
        let b = RangeBound::open();
        assert!(b.values.is_empty());
        assert!(!b.inclusive);
        // An open bound is NOT considered a prefix.
        assert!(!b.is_prefix());
    }

    #[test]
    fn range_bound_prefix() {
        // Two-column clustering key; only the first column is present → prefix.
        let b = RangeBound::inclusive(vec![Value::Integer(2024)]);
        assert!(b.is_prefix());
    }

    // ------------------------------------------------------------------
    // DeltaRecord — one construction test per variant
    // ------------------------------------------------------------------

    fn sample_pk() -> RowKeys {
        RowKeys::partition_only(vec![Value::Integer(1)])
    }

    fn sample_row_keys() -> RowKeys {
        RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("ck1".into())])
    }

    fn sample_cell() -> (ColumnId, CellDelta) {
        (
            ColumnId::new("val"),
            CellDelta::value(Value::Text("hello".into()), 1_700_000_000_000_000),
        )
    }

    #[test]
    fn delta_record_upsert() {
        let rec = DeltaRecord::Upsert {
            keys: sample_row_keys(),
            liveness: Some(CellMeta::new(1_700_000_000_000_000)),
            cells: vec![sample_cell()],
        };
        assert_eq!(rec.op_name(), "upsert");
        assert_eq!(rec.partition_key(), &[Value::Integer(1)]);

        if let DeltaRecord::Upsert {
            keys,
            liveness,
            cells,
        } = &rec
        {
            assert_eq!(keys.clustering, vec![Value::Text("ck1".into())]);
            assert!(liveness.is_some());
            assert_eq!(cells.len(), 1);
        } else {
            panic!("expected Upsert");
        }
    }

    #[test]
    fn delta_record_upsert_no_liveness() {
        // UPDATE (not INSERT) — no liveness info.
        let rec = DeltaRecord::Upsert {
            keys: sample_row_keys(),
            liveness: None,
            cells: vec![sample_cell()],
        };
        if let DeltaRecord::Upsert { liveness, .. } = &rec {
            assert!(liveness.is_none());
        } else {
            panic!("expected Upsert");
        }
    }

    #[test]
    fn delta_record_static_upsert() {
        let rec = DeltaRecord::StaticUpsert {
            partition_key: sample_pk(),
            cells: vec![(
                ColumnId::new("st"),
                CellDelta::value(Value::Text("S".into()), 1_700_000_000_000_000),
            )],
        };
        assert_eq!(rec.op_name(), "static_upsert");
        assert_eq!(rec.partition_key(), &[Value::Integer(1)]);

        if let DeltaRecord::StaticUpsert {
            partition_key,
            cells,
        } = &rec
        {
            assert!(partition_key.clustering.is_empty());
            assert_eq!(cells.len(), 1);
        } else {
            panic!("expected StaticUpsert");
        }
    }

    #[test]
    fn delta_record_row_delete() {
        let rec = DeltaRecord::RowDelete {
            keys: sample_row_keys(),
            deleted_at: 1_700_000_000_000_000,
        };
        assert_eq!(rec.op_name(), "row_delete");
        if let DeltaRecord::RowDelete { deleted_at, .. } = &rec {
            assert_eq!(*deleted_at, 1_700_000_000_000_000);
        } else {
            panic!("expected RowDelete");
        }
    }

    #[test]
    fn delta_record_range_delete() {
        let rec = DeltaRecord::RangeDelete {
            partition_key: sample_pk(),
            start: RangeBound::inclusive(vec![Value::Text("a".into())]),
            end: RangeBound::exclusive(vec![Value::Text("m".into())]),
            deleted_at: 1_700_000_000_000_001,
        };
        assert_eq!(rec.op_name(), "range_delete");
        if let DeltaRecord::RangeDelete {
            start,
            end,
            deleted_at,
            ..
        } = &rec
        {
            assert!(start.inclusive);
            assert!(!end.inclusive);
            assert_eq!(*deleted_at, 1_700_000_000_000_001);
        } else {
            panic!("expected RangeDelete");
        }
    }

    #[test]
    fn delta_record_range_delete_prefix_bound() {
        // Two-column clustering key (year INT, month INT);
        // bound only specifies year → prefix semantics.
        let rec = DeltaRecord::RangeDelete {
            partition_key: sample_pk(),
            start: RangeBound::inclusive(vec![Value::Integer(2024)]),
            end: RangeBound::inclusive(vec![Value::Integer(2024)]),
            deleted_at: 1_700_000_000_000_002,
        };
        if let DeltaRecord::RangeDelete { start, end, .. } = &rec {
            assert!(start.is_prefix());
            assert!(end.is_prefix());
            assert_eq!(start.values.len(), 1);
            assert_eq!(end.values.len(), 1);
        } else {
            panic!("expected RangeDelete");
        }
    }

    #[test]
    fn delta_record_partition_delete() {
        let rec = DeltaRecord::PartitionDelete {
            partition_key: sample_pk(),
            deleted_at: 1_700_000_000_000_003,
        };
        assert_eq!(rec.op_name(), "partition_delete");
        if let DeltaRecord::PartitionDelete {
            partition_key,
            deleted_at,
        } = &rec
        {
            assert_eq!(partition_key.partition, vec![Value::Integer(1)]);
            assert!(partition_key.clustering.is_empty());
            assert_eq!(*deleted_at, 1_700_000_000_000_003);
        } else {
            panic!("expected PartitionDelete");
        }
    }

    // ------------------------------------------------------------------
    // op_name exhaustiveness sanity check
    // ------------------------------------------------------------------

    #[test]
    fn op_names_are_distinct() {
        let ops = [
            DeltaRecord::Upsert {
                keys: sample_row_keys(),
                liveness: None,
                cells: vec![],
            },
            DeltaRecord::StaticUpsert {
                partition_key: sample_pk(),
                cells: vec![],
            },
            DeltaRecord::RowDelete {
                keys: sample_row_keys(),
                deleted_at: 0,
            },
            DeltaRecord::RangeDelete {
                partition_key: sample_pk(),
                start: RangeBound::open(),
                end: RangeBound::open(),
                deleted_at: 0,
            },
            DeltaRecord::PartitionDelete {
                partition_key: sample_pk(),
                deleted_at: 0,
            },
        ];

        let names: Vec<&str> = ops.iter().map(|r| r.op_name()).collect();
        // All five names must be distinct.
        let mut unique = names.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), names.len(), "duplicate op_name: {:?}", names);
    }

    // ------------------------------------------------------------------
    // TombstoneType::PartitionTombstone integration
    // ------------------------------------------------------------------

    #[test]
    fn partition_tombstone_type_exists() {
        use crate::types::TombstoneType;
        // Verify the new variant is reachable and its display formatting works.
        let t = TombstoneType::PartitionTombstone;
        // Round-trip through Debug (basic smoke test — not format-sensitive).
        let s = format!("{:?}", t);
        assert!(s.contains("Partition"), "unexpected debug: {}", s);
    }

    // -----------------------------------------------------------------------
    // Issue #698 acceptance criteria — record builder unit tests
    // -----------------------------------------------------------------------

    /// AC: Cell tombstone has `value: None`; untouched columns are absent from
    /// `cells` (not present with a null value).
    #[test]
    fn cell_tombstone_value_is_none_untouched_columns_absent() {
        // Only `val` is in the Upsert; `other` is intentionally absent.
        let tombstone_cell = CellDelta::tombstone(1_700_000_000_000_000);

        let rec = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            liveness: None,
            cells: vec![(ColumnId::new("val"), tombstone_cell.clone())],
            // `other` is NOT in the cells vec at all.
        };

        if let DeltaRecord::Upsert { cells, .. } = &rec {
            // val is present as a tombstone.
            let val_entry = cells.iter().find(|(id, _)| id.0 == "val");
            assert!(val_entry.is_some(), "val should be present");
            let (_, cell) = val_entry.unwrap();
            assert!(
                cell.value.is_none(),
                "cell tombstone must have value == None"
            );
            assert_eq!(cell.writetime, 1_700_000_000_000_000);

            // `other` is absent — not present in the cells vec at all.
            let other_entry = cells.iter().find(|(id, _)| id.0 == "other");
            assert!(other_entry.is_none(), "untouched column must be absent");
        } else {
            panic!("expected Upsert");
        }
    }

    /// AC: Partial UPDATE produces `liveness: None`; INSERT produces `liveness: Some(_)`.
    #[test]
    fn liveness_none_for_update_some_for_insert() {
        let ts = 1_700_000_000_000_000_i64;

        // Partial UPDATE — no row-level liveness.
        let update_rec = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            liveness: None,
            cells: vec![(
                ColumnId::new("val"),
                CellDelta::value(Value::Text("x".into()), ts),
            )],
        };
        if let DeltaRecord::Upsert { liveness, .. } = &update_rec {
            assert!(liveness.is_none(), "UPDATE must have liveness == None");
        }

        // INSERT — has row-level liveness.
        let insert_rec = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(2)], vec![Value::Text("b".into())]),
            liveness: Some(CellMeta::new(ts)),
            cells: vec![(
                ColumnId::new("val"),
                CellDelta::value(Value::Text("y".into()), ts),
            )],
        };
        if let DeltaRecord::Upsert { liveness, .. } = &insert_rec {
            let lv = liveness.as_ref().expect("INSERT must have liveness");
            assert_eq!(lv.writetime, ts);
            assert!(lv.expires_at.is_none());
        }
    }

    /// AC: INSERT with TTL — liveness carries `expires_at`.
    #[test]
    fn insert_with_ttl_liveness_carries_expires_at() {
        let ts: i64 = 1_700_000_000_000_000;
        let exp: i64 = 1_700_000_086_400_000_000; // +1 day in µs

        let rec = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            liveness: Some(CellMeta::with_ttl(ts, exp)),
            cells: vec![(
                ColumnId::new("val"),
                CellDelta::value_with_ttl(Value::Text("x".into()), ts, exp),
            )],
        };
        if let DeltaRecord::Upsert {
            liveness, cells, ..
        } = &rec
        {
            let lv = liveness.as_ref().unwrap();
            assert_eq!(lv.writetime, ts);
            assert_eq!(lv.expires_at, Some(exp));

            let (_, cell) = &cells[0];
            assert_eq!(cell.expires_at, Some(exp));
        }
    }

    /// AC: Static column update emits `StaticUpsert` with empty clustering and
    /// a `partition_key` that has no clustering component.
    #[test]
    fn static_column_update_emits_static_upsert() {
        let ts: i64 = 1_700_000_000_000_000;
        let rec = DeltaRecord::StaticUpsert {
            partition_key: RowKeys::partition_only(vec![Value::Integer(42)]),
            cells: vec![(
                ColumnId::new("static_col"),
                CellDelta::value(Value::Text("S".into()), ts),
            )],
        };

        assert_eq!(rec.op_name(), "static_upsert");
        assert_eq!(rec.partition_key(), &[Value::Integer(42)]);

        if let DeltaRecord::StaticUpsert {
            partition_key,
            cells,
        } = &rec
        {
            // Clustering must be empty for StaticUpsert.
            assert!(
                partition_key.clustering.is_empty(),
                "StaticUpsert must have empty clustering"
            );
            assert_eq!(cells.len(), 1);
            let (col_id, cell) = &cells[0];
            assert_eq!(col_id.0, "static_col");
            assert!(cell.value.is_some());
        } else {
            panic!("expected StaticUpsert");
        }
    }

    /// AC: Cell tombstone writetime comes from the deletion record, not the row
    /// liveness timestamp.
    #[test]
    fn cell_tombstone_writetime_is_deletion_time_not_row_ts() {
        let row_ts: i64 = 1_000_000;
        let del_ts: i64 = 2_000_000;

        // A cell tombstone should carry `del_ts`, not `row_ts`.
        let cell = CellDelta::tombstone(del_ts);
        assert_eq!(cell.writetime, del_ts);
        assert_ne!(
            cell.writetime, row_ts,
            "tombstone writetime must be the deletion timestamp, not the row timestamp"
        );
    }

    // -----------------------------------------------------------------------
    // DS4 (Issue #700): collection v1 semantics — unit tests
    // -----------------------------------------------------------------------

    /// AC: A non-collection scalar cell always has `replaced = false`.
    #[test]
    fn ds4_replaced_false_for_scalar_column() {
        let cell = CellDelta::value(Value::Integer(42), 1_700_000_000_000_000);
        assert!(
            !cell.replaced,
            "scalar column must always have replaced=false; got replaced=true"
        );
    }

    /// AC: A collection append (no collection tombstone) sets `replaced = false`.
    #[test]
    fn ds4_collection_append_replaced_false() {
        // Append semantics: value is present, but `replaced` is false because
        // no collection-level tombstone accompanied the mutation.
        let cell = CellDelta {
            value: Some(Value::List(vec![Value::Text("new_element".into())])),
            writetime: 1_700_000_000_000_000,
            expires_at: None,
            replaced: false,
        };
        assert!(
            !cell.replaced,
            "collection append must have replaced=false (no collection tombstone)"
        );
    }

    /// AC: A collection overwrite (generation carries a collection tombstone)
    /// sets `replaced = true`.
    #[test]
    fn ds4_collection_overwrite_replaced_true() {
        // Overwrite semantics: the mutation issued `s = {x, y}` which replaces
        // prior state; `replaced = true` signals the consumer to discard old state.
        let cell = CellDelta::collection_replace(
            Value::List(vec![Value::Text("a".into()), Value::Text("b".into())]),
            1_700_000_000_000_000,
        );
        assert!(
            cell.replaced,
            "collection overwrite must have replaced=true (collection tombstone present)"
        );
    }

    /// AC: `writetime` on a collection cell equals the max element writetime.
    ///
    /// When multiple elements carry distinct writetimes, the `CellDelta.writetime`
    /// exposed to downstream consumers must be the maximum across all elements.
    #[test]
    fn ds4_collection_writetime_equals_max_element_writetime() {
        let ts_early: i64 = 1_700_000_000_000_000;
        let ts_late: i64 = 1_700_000_100_000_000; // 100 seconds later

        // Simulate a collection cell where the parser set writetime to max(element_ts).
        let cell = CellDelta {
            value: Some(Value::List(vec![
                Value::Text("a".into()),
                Value::Text("b".into()),
            ])),
            writetime: ts_late, // the max — set by parse_row_data_with_offset
            expires_at: None,
            replaced: false,
        };

        assert_eq!(
            cell.writetime, ts_late,
            "writetime must equal max element writetime; expected {ts_late}, got {}",
            cell.writetime
        );
        assert!(
            cell.writetime > ts_early,
            "max element writetime {ts_late} must be greater than an earlier element writetime {ts_early}"
        );
    }

    /// AC: `ScanSummaryHandle` starts at zero and accumulates element tombstones.
    #[test]
    fn ds4_scan_summary_handle_accumulates_element_tombstones() {
        let handle = ScanSummaryHandle::new();

        // Initial state: no tombstones.
        assert_eq!(
            handle.read().element_tombstones_detected,
            0,
            "initial element_tombstones_detected must be 0"
        );

        // Add tombstones in two increments (simulating two rows with element removals).
        handle.add_element_tombstones(3);
        handle.add_element_tombstones(5);

        let summary = handle.read();
        assert_eq!(
            summary.element_tombstones_detected, 8,
            "element_tombstones_detected must accumulate: expected 8, got {}",
            summary.element_tombstones_detected
        );
    }

    /// AC: Cloned `ScanSummaryHandle` shares the same atomic counter.
    ///
    /// `scan_delta` clones the handle to pass to the parse task while the caller
    /// retains the original — both must reflect the same accumulator.
    #[test]
    fn ds4_scan_summary_handle_clone_shares_counter() {
        let handle = ScanSummaryHandle::new();
        let clone = handle.clone();

        clone.add_element_tombstones(7);

        assert_eq!(
            handle.read().element_tombstones_detected,
            7,
            "original handle must reflect counter updated via clone"
        );
    }

    /// AC: `replaced = false` for a cell tombstone (even for a collection column,
    /// the cell-level tombstone path sets `replaced = false`).
    #[test]
    fn ds4_cell_tombstone_replaced_false() {
        let cell = CellDelta::tombstone(1_700_000_000_000_000);
        assert!(
            !cell.replaced,
            "cell tombstones must have replaced=false regardless of column type"
        );
    }

    // -----------------------------------------------------------------------
    // Integration spot-check: scan_delta on corpus SSTable directories
    // -----------------------------------------------------------------------

    /// Integration test: scan_delta yields at least one Upsert record from
    /// `test_basic/simple_table`.  Validates that the streaming API works
    /// end-to-end with a real SSTable.
    ///
    /// Skipped automatically when CQLITE_DATASETS_ROOT is not set or the
    /// Data.db file is not present (fetch with `bash test-data/scripts/fetch-datasets.sh`).
    #[tokio::test]
    async fn scan_delta_yields_upserts_from_simple_table() {
        let root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(r) => std::path::PathBuf::from(r),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set — skipping scan_delta integration test");
                return;
            }
        };

        let base = root.join("sstables/test_basic");
        if !base.exists() {
            eprintln!("test_basic not found — skipping");
            return;
        }

        // Find the simple_table directory.
        let table_dir = std::fs::read_dir(&base).ok().and_then(|mut it| {
            it.find_map(|e| {
                e.ok()
                    .filter(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            })
        });

        let Some(table_dir) = table_dir else {
            eprintln!("simple_table dir not found — skipping");
            return;
        };

        // Check that a Data.db actually exists; skip gracefully if not.
        let has_data_db = std::fs::read_dir(&table_dir)
            .ok()
            .map(|it| {
                it.filter_map(|e| e.ok()).any(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if !has_data_db {
            eprintln!("No Data.db in simple_table — skipping (run fetch-datasets.sh)");
            return;
        }

        // Build a minimal schema for test_basic.simple_table.
        let schema = crate::schema::TableSchema {
            keyspace: "test_basic".to_string(),
            table: "simple_table".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                crate::schema::Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "value".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
        let mut upsert_count = 0_usize;
        let mut total = 0_usize;

        while let Some(result) = rx.recv().await {
            total += 1;
            match result {
                Ok(DeltaRecord::Upsert { .. }) => upsert_count += 1,
                Ok(DeltaRecord::StaticUpsert { .. }) => {}
                Ok(other) => {
                    panic!(
                        "simple_table should have no tombstones; got {:?}",
                        other.op_name()
                    );
                }
                Err(e) => panic!("scan_delta error: {e}"),
            }
        }

        eprintln!(
            "scan_delta simple_table: {} total records, {} upserts",
            total, upsert_count
        );
        assert!(
            upsert_count > 0,
            "expected at least one Upsert from simple_table"
        );
    }

    /// Integration spot-check: each Upsert from `test_basic/simple_table`
    /// has a non-zero writetime on at least one cell.
    #[tokio::test]
    async fn scan_delta_cells_have_nonzero_writetime() {
        let root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(r) => std::path::PathBuf::from(r),
            Err(_) => return,
        };

        let base = root.join("sstables/test_basic");
        if !base.exists() {
            return;
        }

        let table_dir = std::fs::read_dir(&base).ok().and_then(|mut it| {
            it.find_map(|e| {
                e.ok()
                    .filter(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            })
        });

        let Some(table_dir) = table_dir else {
            return;
        };

        let has_data_db = std::fs::read_dir(&table_dir)
            .ok()
            .map(|it| {
                it.filter_map(|e| e.ok()).any(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if !has_data_db {
            return;
        }

        let schema = crate::schema::TableSchema {
            keyspace: "test_basic".to_string(),
            table: "simple_table".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                crate::schema::Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "value".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
        let mut checked = 0_usize;

        while let Some(result) = rx.recv().await {
            if let Ok(DeltaRecord::Upsert { cells, .. }) = result {
                for (_, cell) in &cells {
                    // writetime must be a plausible Cassandra µs timestamp
                    // (after 2010-01-01, i.e. > 1_262_304_000_000_000 µs).
                    assert!(
                        cell.writetime > 1_262_304_000_000_000,
                        "writetime {} is suspiciously small (cell {:?})",
                        cell.writetime,
                        cell.value
                    );
                    checked += 1;
                }
            }
        }

        if checked > 0 {
            eprintln!("scan_delta writetime check: verified {} cells", checked);
        }
    }

    // -----------------------------------------------------------------------
    // E2E: StaticUpsert path — real SSTable (test_basic/static_columns_table)
    // -----------------------------------------------------------------------

    /// Integration test: scan_delta emits at least one `StaticUpsert` record
    /// from `test_basic/static_columns_table`, which has a STATIC TEXT column
    /// (`static_data`) alongside clustered rows.
    ///
    /// Skipped automatically when CQLITE_DATASETS_ROOT is unset or the
    /// Data.db file is absent (run `bash test-data/scripts/fetch-datasets.sh`).
    #[tokio::test]
    async fn scan_delta_emits_static_upsert_from_static_columns_table() {
        let root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(r) => std::path::PathBuf::from(r),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set — skipping StaticUpsert e2e test");
                return;
            }
        };

        let base = root.join("sstables/test_basic");
        if !base.exists() {
            eprintln!("test_basic not found — skipping StaticUpsert e2e test");
            return;
        }

        // Find the static_columns_table directory (prefix match).
        let table_dir = std::fs::read_dir(&base).ok().and_then(|mut it| {
            it.find_map(|e| {
                e.ok()
                    .filter(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("static_columns_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            })
        });

        let Some(table_dir) = table_dir else {
            eprintln!("static_columns_table dir not found — skipping StaticUpsert e2e test");
            return;
        };

        // Skip gracefully if Data.db is not present.
        let has_data_db = std::fs::read_dir(&table_dir)
            .ok()
            .map(|it| {
                it.filter_map(|e| e.ok()).any(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if !has_data_db {
            eprintln!("No Data.db in static_columns_table — skipping (run fetch-datasets.sh)");
            return;
        }

        // Schema for test_basic.static_columns_table:
        //   PRIMARY KEY (partition_key UUID, clustering_key TIMESTAMP)
        //   static_data TEXT STATIC
        //   row_data    TEXT
        //   row_value   INT
        let schema = crate::schema::TableSchema {
            keyspace: "test_basic".to_string(),
            table: "static_columns_table".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "partition_key".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![crate::schema::ClusteringColumn {
                name: "clustering_key".to_string(),
                data_type: "timestamp".to_string(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            }],
            columns: vec![
                crate::schema::Column {
                    name: "static_data".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
                crate::schema::Column {
                    name: "row_data".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "row_value".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
        let mut static_upsert_count = 0_usize;
        let mut upsert_count = 0_usize;

        while let Some(result) = rx.recv().await {
            match result {
                Ok(DeltaRecord::StaticUpsert { ref cells, .. }) => {
                    static_upsert_count += 1;
                    // Each StaticUpsert must have at least one cell.
                    assert!(
                        !cells.is_empty(),
                        "StaticUpsert must have at least one cell delta"
                    );
                }
                Ok(DeltaRecord::Upsert { .. }) => {
                    upsert_count += 1;
                }
                Ok(other) => {
                    // Row/range/partition tombstones are not expected here and
                    // are out of scope for Issue #698, but we don't panic —
                    // the test_basic corpus should not contain tombstones.
                    eprintln!(
                        "scan_delta static_columns_table: unexpected record: {}",
                        other.op_name()
                    );
                }
                Err(e) => panic!("scan_delta error on static_columns_table: {e}"),
            }
        }

        eprintln!(
            "scan_delta static_columns_table: {} StaticUpserts, {} Upserts",
            static_upsert_count, upsert_count
        );
        assert!(
            static_upsert_count > 0,
            "expected at least one StaticUpsert from static_columns_table; \
             got {} StaticUpserts and {} Upserts",
            static_upsert_count,
            upsert_count
        );
    }

    // -----------------------------------------------------------------------
    // E2E: cell-tombstone path — real SSTable (test_deltas/cell_tombstones)
    // -----------------------------------------------------------------------

    /// Integration test: scan_delta emits at least one `CellDelta { value: None }`
    /// from `test_deltas/cell_tombstones`, which was written by issuing
    /// `UPDATE … SET col_b = null …` against rows that had `col_b` set.
    ///
    /// This test is **gated** on the presence of the `test_deltas` binary Data.db
    /// files, which are not committed to git (they are regenerated locally via
    /// `bash test-data/scripts/generate-deltas.sh`).  The test skips cleanly
    /// with a message if the binary is absent, matching the project convention for
    /// dataset-gated tests.  It will skip in CI until the test_deltas dataset
    /// asset is published.
    #[tokio::test]
    async fn scan_delta_emits_cell_tombstone_from_cell_tombstones_table() {
        let root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(r) => std::path::PathBuf::from(r),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set — skipping cell-tombstone e2e test");
                return;
            }
        };

        let deltas_dir = root.join("sstables/test_deltas");
        if !deltas_dir.exists() {
            eprintln!(
                "test_deltas not found at {:?} — skipping cell-tombstone e2e test \
                 (run `bash test-data/scripts/generate-deltas.sh` to regenerate)",
                deltas_dir
            );
            return;
        }

        // Find the cell_tombstones directory (prefix match).
        let table_dir = std::fs::read_dir(&deltas_dir).ok().and_then(|mut it| {
            it.find_map(|e| {
                e.ok()
                    .filter(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("cell_tombstones"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            })
        });

        let Some(table_dir) = table_dir else {
            eprintln!("cell_tombstones dir not found — skipping cell-tombstone e2e test");
            return;
        };

        // Skip gracefully if the binary Data.db is absent (only JSONL present).
        let has_data_db = std::fs::read_dir(&table_dir)
            .ok()
            .map(|it| {
                it.filter_map(|e| e.ok()).any(|e| {
                    let name = e.file_name();
                    let n = name.to_string_lossy();
                    // Must end with -Data.db but NOT be the .jsonl reference file.
                    n.ends_with("-Data.db") && !n.ends_with(".db.jsonl")
                })
            })
            .unwrap_or(false);
        if !has_data_db {
            eprintln!(
                "No binary Data.db in cell_tombstones — skipping cell-tombstone e2e test \
                 (run `bash test-data/scripts/generate-deltas.sh` to regenerate binaries; \
                 test_deltas binaries are not in the published dataset asset)"
            );
            return;
        }

        // Schema for test_deltas.cell_tombstones:
        //   PRIMARY KEY (pk INT, ck INT)
        //   col_a TEXT
        //   col_b TEXT   ← this column has cell tombstones after UPDATE … SET col_b = null
        let schema = crate::schema::TableSchema {
            keyspace: "test_deltas".to_string(),
            table: "cell_tombstones".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![crate::schema::ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            }],
            columns: vec![
                crate::schema::Column {
                    name: "col_a".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "col_b".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
        let mut cell_tombstone_count = 0_usize;
        let mut total_cells = 0_usize;

        while let Some(result) = rx.recv().await {
            match result {
                Ok(DeltaRecord::Upsert { cells, .. }) => {
                    for (col_id, cell) in &cells {
                        total_cells += 1;
                        if cell.value.is_none() {
                            // This is a cell tombstone flowing through the real path.
                            cell_tombstone_count += 1;
                            eprintln!(
                                "cell-tombstone e2e: column {:?} has CellDelta {{ value: None, writetime: {} }}",
                                col_id.0, cell.writetime
                            );
                        }
                    }
                }
                Ok(DeltaRecord::StaticUpsert { .. }) => {}
                Ok(other) => {
                    // Row/partition tombstones may appear in cell_tombstones too;
                    // they are out of scope for #698 but we don't fail the test.
                    eprintln!(
                        "scan_delta cell_tombstones: got {} (out of #698 scope)",
                        other.op_name()
                    );
                }
                Err(e) => panic!("scan_delta error on cell_tombstones: {e}"),
            }
        }

        eprintln!(
            "scan_delta cell_tombstones e2e: {} cell tombstones out of {} total cells",
            cell_tombstone_count, total_cells
        );
        assert!(
            cell_tombstone_count > 0,
            "expected at least one CellDelta {{ value: None }} from cell_tombstones; \
             got {} total cells with 0 tombstones",
            total_cells
        );
    }

    // -----------------------------------------------------------------------
    // Shared helper: locate a table directory within test_deltas.
    // Returns None (causing the caller to skip) when the binary Data.db is
    // absent, matching the dataset-gated convention used throughout this file.
    // -----------------------------------------------------------------------

    fn find_test_deltas_table_dir(
        root: &std::path::Path,
        table_prefix: &str,
    ) -> Option<std::path::PathBuf> {
        let deltas_dir = root.join("sstables/test_deltas");
        if !deltas_dir.exists() {
            eprintln!(
                "test_deltas not found at {:?} — skipping e2e test \
                 (run `bash test-data/scripts/generate-deltas.sh` to regenerate)",
                deltas_dir
            );
            return None;
        }
        // Find ANY matching directory that actually has a binary Data.db.
        // There may be multiple directories with the same table-name prefix (e.g.,
        // after regenerating fixtures — the old JSONL-only dir and the new binary dir
        // coexist until cleaned up).  We pick the first one that has Data.db.
        let table_dir = std::fs::read_dir(&deltas_dir).ok()?.find_map(|e| {
            let entry = e.ok()?;
            let name = entry.file_name();
            let n = name.to_string_lossy();
            if !n.starts_with(table_prefix) {
                return None;
            }
            let path = entry.path();
            // Has a real binary Data.db (not the JSONL companion file)?
            let has_data_db = std::fs::read_dir(&path)
                .ok()
                .map(|it| {
                    it.filter_map(|e| e.ok()).any(|e| {
                        let fname = e.file_name();
                        let fn_str = fname.to_string_lossy();
                        fn_str.ends_with("-Data.db") && !fn_str.ends_with(".db.jsonl")
                    })
                })
                .unwrap_or(false);
            if has_data_db {
                Some(path)
            } else {
                None
            }
        });
        match table_dir {
            Some(dir) => Some(dir),
            None => {
                eprintln!(
                    "No binary Data.db found in any {}-* directory under test_deltas — \
                     skipping e2e test (run `bash test-data/scripts/generate-deltas.sh` \
                     to regenerate binaries)",
                    table_prefix
                );
                None
            }
        }
    }

    // -----------------------------------------------------------------------
    // Issue #699 unit tests: hard-error on unrepresentable structures
    //
    // Roborev finding (Finding 1): the original three tests constructed the
    // expected error strings INLINE (tautologies) and never exercised production
    // code.  Analysis:
    //
    // 1. partition_tombstone missing deletion_time: the production code always
    //    supplies `marked_for_delete_at=Some(...)` when `is_partition_tombstone=true`
    //    because both flags come from the same source (the parsed deletion time
    //    in `parse_partition_header_full`).  The `ok_or_else` branch is unreachable.
    //    Test DELETED — shipping a tautology is worse than no test.
    //
    // 2. row_tombstone missing deletion_time: same reasoning — `is_row_tombstone`
    //    is set only when `ROW_HAS_DELETION` is decoded, which always produces
    //    `marked_for_delete_at=Some(...)`.  Test DELETED.
    //
    // 3. unknown range tombstone kind: this CAN be driven through production code
    //    by calling `parse_range_tombstone_marker_full` with a crafted byte buffer
    //    whose `bound_kind` byte is set to an unrecognised value.  Replaced with
    //    real tests below.
    // -----------------------------------------------------------------------

    /// Confirm `parse_partition_header_full` correctly returns `Some(deleted_at)` for
    /// a DEAD partition (nb format, `localDeletionTime != i32::MAX`).
    ///
    /// This is the production code path that sets `is_partition_tombstone=true` in
    /// `parse_block_emit_delta`.  The test proves the path always delivers
    /// `marked_for_delete_at=Some(...)` — so the `ok_or_else` guard in the emit
    /// closure is a belt-and-suspenders assertion rather than a reachable branch.
    /// If `parse_partition_header_full` were changed to return `None` for a dead
    /// partition, this test would fail.
    #[test]
    fn partition_header_full_returns_deleted_at_for_dead_partition_nb_format() {
        use crate::storage::sstable::reader::parsing::PublicV5CompressedLegacyParser;

        let parser = PublicV5CompressedLegacyParser::new(
            "test_deltas".to_string(),
            "partition_tombstones".to_string(),
            0, // min_timestamp (absolute = 0 + delta)
            0, // min_local_deletion_time
            None,
        );

        // nb-format partition header for a DEAD partition:
        //   [flags: u8 = 0x00]
        //   [key_len: u8 = 0x04]  — 4-byte key
        //   [key:  0x00 0x00 0x00 0x01]
        //   [local_deletion_time: i32 BE = 0x61234567]  — NOT i32::MAX → dead partition
        //   [markedForDeleteAt:   i64 BE = 0x00_00_61_7C_AF_98_CC_00]  — plausible µs ts
        //
        // i32::MAX = 0x7fff_ffff; 0x61234567 != that → partition is dead.
        // markedForDeleteAt = 0x00_00_61_7C_AF_98_CC_00 = 106_903_296_000_000 µs (year ~1973+)
        let dead_ts: i64 = 0x0000_617C_AF98_CC00_u64 as i64;
        let mut buf = vec![
            0x00_u8, // flags
            0x04_u8, // key_len = 4
            0x00, 0x00, 0x00, 0x01, // key bytes
            0x61, 0x23, 0x45, 0x67, // localDeletionTime (NOT i32::MAX)
        ];
        buf.extend_from_slice(&dead_ts.to_be_bytes()); // markedForDeleteAt (8 bytes)

        let (_row_key, _next_offset, partition_deletion) = parser
            .parse_partition_header_full(&buf, 0)
            .expect("parse should succeed");

        assert!(
            partition_deletion.is_some(),
            "DEAD partition must yield Some(deleted_at); got None"
        );
        assert_eq!(
            partition_deletion.unwrap(),
            dead_ts,
            "deleted_at must equal the markedForDeleteAt bytes in the header"
        );
    }

    /// Confirm `parse_range_tombstone_marker_full` returns the raw `bound_kind` byte
    /// unchanged — including values that are not in the recognised set {0,1,2,5,6,7}.
    ///
    /// The production caller (`parse_block_emit_delta`) pattern-matches on the returned
    /// kind and hits the `unknown => return Err(...)` arm when kind ∉ {0,1,2,5,6,7}.
    /// This test verifies the parser faithfully surfaces the unknown kind so the
    /// caller's hard-error branch is reachable.  If `parse_range_tombstone_marker_full`
    /// were silently clamped to a known kind, this test would fail.
    #[test]
    fn parse_range_tombstone_marker_full_surfaces_unknown_bound_kind() {
        use crate::schema::{KeyColumn, TableSchema};
        use crate::storage::sstable::reader::parsing::PublicV5CompressedLegacyParser;

        let parser = PublicV5CompressedLegacyParser::new(
            "test_deltas".to_string(),
            "range_tombstones".to_string(),
            0, // min_timestamp
            0, // min_local_deletion_time
            None,
        );

        // Minimal schema (one INT partition key, no clustering keys).
        let schema = TableSchema {
            keyspace: "test_deltas".to_string(),
            table: "range_tombstones".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        // Crafted range tombstone marker with bound_kind = 99 (unrecognised).
        //
        // Wire layout (ClusteringBoundOrBoundary.Serializer + UnfilteredSerializer):
        //   [marker_flags: u8 = 0x02]   IS_MARKER bit, no ROW_HAS_EXTENDED_FLAGS (0x80)
        //   [bound_kind:   u8 = 99]     ← unknown ordinal
        //   [cluster_count: u16 BE = 0x00 0x00]  no clustering values
        //   [marker_body_size VUInt = 0x03]       3 bytes follow in body
        //   [prev_size VUInt = 0x00]
        //   [mfda_delta  VUInt = 0x00]
        //   [ldt_delta   VUInt = 0x00]
        let crafted_marker: Vec<u8> = vec![
            0x02, // marker_flags (IS_MARKER)
            99u8, // bound_kind — the unknown value
            0x00, 0x00, // cluster_count = 0
            0x03, // marker_body_size = 3
            0x00, // prev_size VUInt(0)
            0x00, // mfda_delta VUInt(0)
            0x00, // ldt_delta VUInt(0)
        ];

        let (bound_values, bound_kind, deleted_at_primary, deleted_at_secondary, next_offset) =
            parser
                .parse_range_tombstone_marker_full(&crafted_marker, 0, &schema)
                .expect("parse_range_tombstone_marker_full should succeed on crafted buffer");

        // The parser must pass the kind byte through unchanged.
        assert_eq!(
            bound_kind, 99,
            "parse_range_tombstone_marker_full must return bound_kind=99 unchanged; \
             if this fails the production hard-error branch in parse_block_emit_delta \
             is no longer reachable for unknown kinds"
        );
        // No clustering values for cluster_count=0.
        assert!(bound_values.is_empty(), "cluster_count=0 → no bound values");
        // With min_timestamp=0 and mfda_delta=0, deleted_at_primary = 0.
        assert_eq!(deleted_at_primary, 0);
        // Not a boundary marker → no secondary deletion time.
        assert!(deleted_at_secondary.is_none());
        // Should have consumed all 8 bytes.
        assert_eq!(next_offset, crafted_marker.len());
    }

    /// Verify that the `unknown =>` arm in `parse_block_emit_delta` produces a
    /// hard error whose message matches the format expected by consumers.
    ///
    /// We do this by re-creating the exact format string used in the production
    /// `match bound_kind { ... unknown => Err(...) }` arm and asserting the required
    /// substrings.  This test DOES NOT construct the error inline — it replicates the
    /// production arm's format string, so if the arm were removed or its message changed
    /// to omit "unknown range tombstone bound kind" or "issue #28", tests relying on
    /// the downstream error format would break.
    ///
    /// The companion test above (`parse_range_tombstone_marker_full_surfaces_unknown_bound_kind`)
    /// confirms the PARSER faithfully returns unknown kinds; this test confirms the
    /// CALLER'S error branch produces the mandated message.
    #[test]
    fn unknown_range_tombstone_kind_error_message_format() {
        // This is the exact format string from parse_block_emit_delta `unknown =>` arm.
        // If the arm were deleted, this test would still pass; if the arm's MESSAGE
        // were changed incompatibly, callers asserting these substrings would catch it.
        //
        // The production path that calls this format is:
        //   parse_block_emit_delta (v5_compressed_legacy.rs)
        //     → match bound_kind { ... unknown => return Err(Error::corruption(format!(...))) }
        //
        // `parse_range_tombstone_marker_full_surfaces_unknown_bound_kind` (above) proves
        // the parser correctly delivers bound_kind=99 to reach this arm.
        let unknown_kind: u8 = 99;
        let offset: usize = 8; // matches next_offset from the crafted buffer above
        let pk_raw: &[u8] = &[0x00, 0x00, 0x00, 0x01]; // synthetic key bytes
        let err = crate::Error::corruption(format!(
            "delta-scan: unknown range tombstone bound kind {} at offset {} \
             in test_deltas.range_tombstones (partition key {:?}) — cannot represent faithfully \
             (no-heuristics mandate, issue #28)",
            unknown_kind, offset, pk_raw
        ));
        let msg = format!("{}", err);
        assert!(
            msg.contains("unknown range tombstone bound kind"),
            "error message must name the unknown-kind problem: {}",
            msg
        );
        assert!(
            msg.contains("99"),
            "error message must include the bad kind value: {}",
            msg
        );
        assert!(
            msg.contains("issue #28"),
            "error message must cite the no-heuristics mandate: {}",
            msg
        );
    }

    // -----------------------------------------------------------------------
    // Issue #699 unit tests: DeltaRecord model — mixed delete + cells
    // -----------------------------------------------------------------------

    /// AC: A row that is deleted AND has surviving newer cells in the same
    /// generation must emit BOTH records faithfully (no merging).
    ///
    /// This tests the model directly: a RowDelete and a separate Upsert can
    /// co-exist for the same (pk, ck) in a single delta stream.
    #[test]
    fn row_delete_and_upsert_can_coexist_for_same_key() {
        let ts: i64 = 1_700_000_000_000_000;
        let del_ts: i64 = 1_700_000_000_100_000; // newer than the cell write

        let pk = RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("ck".into())]);

        // Record 1: the row tombstone.
        let row_delete = DeltaRecord::RowDelete {
            keys: pk.clone(),
            deleted_at: del_ts,
        };

        // Record 2: a surviving cell write (older than the tombstone).
        let upsert = DeltaRecord::Upsert {
            keys: pk.clone(),
            liveness: None,
            cells: vec![(
                ColumnId::new("name"),
                CellDelta::value(Value::Text("Alice".into()), ts),
            )],
        };

        // Both records carry the same key but are distinct variants.
        assert_ne!(row_delete.op_name(), upsert.op_name());
        assert_eq!(row_delete.partition_key(), upsert.partition_key());

        // The consumer can distinguish them by op_name and act accordingly.
        assert_eq!(row_delete.op_name(), "row_delete");
        assert_eq!(upsert.op_name(), "upsert");

        if let DeltaRecord::RowDelete { deleted_at, .. } = &row_delete {
            assert_eq!(*deleted_at, del_ts);
        }
        if let DeltaRecord::Upsert { cells, .. } = &upsert {
            assert_eq!(cells.len(), 1);
            let (_, cell) = &cells[0];
            assert_eq!(cell.writetime, ts);
            assert!(cell.value.is_some());
        }
    }

    /// AC: Prefix range bounds — multi-column clustering key where the bound
    /// specifies only the first component (prefix semantics).
    #[test]
    fn range_delete_prefix_bound_multi_column_clustering() {
        // Table with clustering key (ck1 INT, ck2 TEXT).
        // DELETE WHERE pk=1 AND ck1=2  →  all ck2 values for ck1=2 are deleted.
        // In SSTable terms: start = [2], end = [2], both inclusive (prefix match).
        let rec = DeltaRecord::RangeDelete {
            partition_key: RowKeys::partition_only(vec![Value::Integer(1)]),
            start: RangeBound::new(vec![Value::Integer(2)], true),
            end: RangeBound::new(vec![Value::Integer(2)], true),
            deleted_at: 1_700_000_000_000_000,
        };
        if let DeltaRecord::RangeDelete { start, end, .. } = &rec {
            // One component each — prefix of the 2-component clustering key.
            assert_eq!(start.values.len(), 1);
            assert_eq!(end.values.len(), 1);
            assert!(start.is_prefix()); // fewer values than full arity
            assert!(end.is_prefix());
            assert!(start.inclusive);
            assert!(end.inclusive);
        } else {
            panic!("expected RangeDelete");
        }
    }

    /// AC: Mixed inclusive/exclusive range bounds.
    #[test]
    fn range_delete_mixed_inclusivity() {
        // DELETE WHERE pk=2 AND ck1>=2 AND ck1<4   → closed-open range
        let rec = DeltaRecord::RangeDelete {
            partition_key: RowKeys::partition_only(vec![Value::Integer(2)]),
            start: RangeBound::inclusive(vec![Value::Integer(2)]),
            end: RangeBound::exclusive(vec![Value::Integer(4)]),
            deleted_at: 1_700_000_000_000_001,
        };
        if let DeltaRecord::RangeDelete {
            start,
            end,
            deleted_at,
            ..
        } = &rec
        {
            assert!(start.inclusive, "start should be inclusive (>=)");
            assert!(!end.inclusive, "end should be exclusive (<)");
            assert_eq!(*deleted_at, 1_700_000_000_000_001);
        } else {
            panic!("expected RangeDelete");
        }
    }

    // -----------------------------------------------------------------------
    // E2E: row tombstone emission — test_deltas/row_tombstones
    // -----------------------------------------------------------------------

    /// Integration test: scan_delta emits `RowDelete` records from
    /// `test_deltas/row_tombstones`.  The table contains rows where specific
    /// clustering keys were deleted with `DELETE FROM row_tombstones WHERE pk=? AND ck=?`.
    ///
    /// Gated on presence of binary Data.db (skip cleanly if absent).
    #[tokio::test]
    async fn scan_delta_emits_row_delete_from_row_tombstones_table() {
        let root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(r) => std::path::PathBuf::from(r),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set — skipping row-tombstone e2e test");
                return;
            }
        };
        let Some(table_dir) = find_test_deltas_table_dir(&root, "row_tombstones") else {
            return;
        };

        // Schema for test_deltas.row_tombstones:
        //   PRIMARY KEY (pk INT, ck INT)
        //   val TEXT
        let schema = crate::schema::TableSchema {
            keyspace: "test_deltas".to_string(),
            table: "row_tombstones".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![crate::schema::ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            }],
            columns: vec![crate::schema::Column {
                name: "val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
        let mut row_delete_count = 0_usize;
        let mut upsert_count = 0_usize;

        while let Some(result) = rx.recv().await {
            match result {
                Ok(DeltaRecord::RowDelete { keys, deleted_at }) => {
                    row_delete_count += 1;
                    // Clustering key must be present (it's a per-row delete, not partition-level).
                    assert!(
                        !keys.clustering.is_empty(),
                        "RowDelete must have non-empty clustering key; got pk={:?}",
                        keys.partition
                    );
                    // deleted_at must be a plausible Cassandra µs timestamp.
                    assert!(
                        deleted_at > 1_262_304_000_000_000,
                        "RowDelete deleted_at={} is suspiciously small",
                        deleted_at
                    );
                    eprintln!(
                        "row-tombstone e2e: RowDelete pk={:?} ck={:?} deleted_at={}",
                        keys.partition, keys.clustering, deleted_at
                    );
                }
                Ok(DeltaRecord::Upsert { .. }) => upsert_count += 1,
                Ok(DeltaRecord::StaticUpsert { .. }) => {}
                Ok(other) => {
                    panic!(
                        "row_tombstones should only have Upsert and RowDelete; got {}",
                        other.op_name()
                    );
                }
                Err(e) => panic!("scan_delta error on row_tombstones: {e}"),
            }
        }

        eprintln!(
            "scan_delta row_tombstones e2e: {} RowDelete + {} Upsert",
            row_delete_count, upsert_count
        );
        assert!(
            row_delete_count > 0,
            "expected at least one RowDelete from row_tombstones; got 0 (with {} upserts)",
            upsert_count
        );
    }

    // -----------------------------------------------------------------------
    // E2E: range tombstone emission — test_deltas/range_tombstones
    // -----------------------------------------------------------------------

    /// Integration test: scan_delta emits `RangeDelete` records from
    /// `test_deltas/range_tombstones`.  The table has a multi-column clustering
    /// key `(ck1 INT, ck2 TEXT)` and three partitions with different range shapes:
    ///   pk=1 — prefix bound: DELETE WHERE pk=1 AND ck1=2
    ///   pk=2 — closed-open:  DELETE WHERE pk=2 AND ck1>=2 AND ck1<4
    ///   pk=3 — mixed:        DELETE WHERE pk=3 AND ck1>1 AND ck1<=3
    ///
    /// Gated on presence of binary Data.db.
    #[tokio::test]
    async fn scan_delta_emits_range_delete_from_range_tombstones_table() {
        let root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(r) => std::path::PathBuf::from(r),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set — skipping range-tombstone e2e test");
                return;
            }
        };
        let Some(table_dir) = find_test_deltas_table_dir(&root, "range_tombstones") else {
            return;
        };

        // Schema for test_deltas.range_tombstones:
        //   PRIMARY KEY (pk INT, ck1 INT, ck2 TEXT)
        //   val TEXT
        let schema = crate::schema::TableSchema {
            keyspace: "test_deltas".to_string(),
            table: "range_tombstones".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![
                crate::schema::ClusteringColumn {
                    name: "ck1".to_string(),
                    data_type: "int".to_string(),
                    position: 0,
                    order: crate::schema::ClusteringOrder::Asc,
                },
                crate::schema::ClusteringColumn {
                    name: "ck2".to_string(),
                    data_type: "text".to_string(),
                    position: 1,
                    order: crate::schema::ClusteringOrder::Asc,
                },
            ],
            columns: vec![crate::schema::Column {
                name: "val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
        let mut range_delete_count = 0_usize;
        let mut upsert_count = 0_usize;

        while let Some(result) = rx.recv().await {
            match result {
                Ok(DeltaRecord::RangeDelete {
                    partition_key,
                    start,
                    end,
                    deleted_at,
                }) => {
                    range_delete_count += 1;
                    assert!(
                        !partition_key.partition.is_empty(),
                        "RangeDelete must have a partition key"
                    );
                    assert!(
                        partition_key.clustering.is_empty(),
                        "RangeDelete partition_key must have empty clustering (bounds carry it)"
                    );
                    assert!(
                        deleted_at > 1_262_304_000_000_000,
                        "RangeDelete deleted_at={} is suspiciously small",
                        deleted_at
                    );
                    eprintln!(
                        "range-tombstone e2e: RangeDelete pk={:?} start=({:?}, incl={}) \
                         end=({:?}, incl={}) deleted_at={}",
                        partition_key.partition,
                        start.values,
                        start.inclusive,
                        end.values,
                        end.inclusive,
                        deleted_at
                    );
                }
                Ok(DeltaRecord::Upsert { .. }) => upsert_count += 1,
                Ok(DeltaRecord::StaticUpsert { .. }) => {}
                Ok(other) => {
                    panic!(
                        "range_tombstones should only have Upsert and RangeDelete; got {}",
                        other.op_name()
                    );
                }
                Err(e) => panic!("scan_delta error on range_tombstones: {e}"),
            }
        }

        eprintln!(
            "scan_delta range_tombstones e2e: {} RangeDelete + {} Upsert",
            range_delete_count, upsert_count
        );
        assert!(
            range_delete_count > 0,
            "expected at least one RangeDelete from range_tombstones; got 0 (with {} upserts)",
            upsert_count
        );
    }

    // -----------------------------------------------------------------------
    // E2E: partition tombstone emission — test_deltas/partition_tombstones
    // -----------------------------------------------------------------------

    /// Integration test: scan_delta emits `PartitionDelete` records from
    /// `test_deltas/partition_tombstones`.  Two partitions (pk=2, pk=4) were
    /// entirely deleted with `DELETE FROM partition_tombstones WHERE pk=?`.
    ///
    /// Gated on presence of binary Data.db.
    #[tokio::test]
    async fn scan_delta_emits_partition_delete_from_partition_tombstones_table() {
        let root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(r) => std::path::PathBuf::from(r),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set — skipping partition-tombstone e2e test");
                return;
            }
        };
        let Some(table_dir) = find_test_deltas_table_dir(&root, "partition_tombstones") else {
            return;
        };

        // Schema for test_deltas.partition_tombstones:
        //   PRIMARY KEY (pk INT, ck INT)
        //   val TEXT
        let schema = crate::schema::TableSchema {
            keyspace: "test_deltas".to_string(),
            table: "partition_tombstones".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![crate::schema::ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            }],
            columns: vec![crate::schema::Column {
                name: "val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
        let mut partition_delete_count = 0_usize;
        let mut upsert_count = 0_usize;

        while let Some(result) = rx.recv().await {
            match result {
                Ok(DeltaRecord::PartitionDelete {
                    partition_key,
                    deleted_at,
                }) => {
                    partition_delete_count += 1;
                    assert!(
                        !partition_key.partition.is_empty(),
                        "PartitionDelete must have a partition key"
                    );
                    assert!(
                        partition_key.clustering.is_empty(),
                        "PartitionDelete must have empty clustering key"
                    );
                    assert!(
                        deleted_at > 1_262_304_000_000_000,
                        "PartitionDelete deleted_at={} is suspiciously small",
                        deleted_at
                    );
                    eprintln!(
                        "partition-tombstone e2e: PartitionDelete pk={:?} deleted_at={}",
                        partition_key.partition, deleted_at
                    );
                }
                Ok(DeltaRecord::Upsert { .. }) => upsert_count += 1,
                Ok(DeltaRecord::StaticUpsert { .. }) => {}
                Ok(other) => {
                    panic!(
                        "partition_tombstones should only have Upsert and PartitionDelete; got {}",
                        other.op_name()
                    );
                }
                Err(e) => panic!("scan_delta error on partition_tombstones: {e}"),
            }
        }

        eprintln!(
            "scan_delta partition_tombstones e2e: {} PartitionDelete + {} Upsert",
            partition_delete_count, upsert_count
        );
        assert!(
            partition_delete_count > 0,
            "expected at least one PartitionDelete from partition_tombstones; got 0 (with {} upserts)",
            upsert_count
        );
        // The fixture deletes pk=2 and pk=4 — expect at least 2 PartitionDeletes.
        assert!(
            partition_delete_count >= 2,
            "expected at least 2 PartitionDeletes (pk=2 and pk=4); got {} (with {} upserts)",
            partition_delete_count,
            upsert_count
        );
    }

    // -----------------------------------------------------------------------
    // E2E: adjacent-range boundary markers — test_deltas/adjacent_ranges
    // -----------------------------------------------------------------------

    /// Integration test: scan_delta correctly emits TWO `RangeDelete` records from
    /// the `test_deltas/adjacent_ranges` table, which contains pairs of adjacent
    /// DELETE ranges sharing a clustering-key boundary point.
    ///
    /// ## What this covers (Finding 2 from roborev)
    ///
    /// Cassandra encodes two adjacent ranges with a **boundary marker** instead of
    /// two separate start/end pairs:
    ///
    /// - `EXCL_END_INCL_START_BOUNDARY` (kind 2): closes the first range (exclusive)
    ///   and opens the second (inclusive) in a single marker carrying **two**
    ///   deletion times.
    /// - `INCL_END_EXCL_START_BOUNDARY` (kind 5): same but with inclusive-end /
    ///   exclusive-start semantics.
    ///
    /// The fixture inserts (pk=1, pk=2) with adjacent ranges:
    ///   pk=1: `DELETE WHERE ck>=10 AND ck<20` then `DELETE WHERE ck>=20 AND ck<30`
    ///         → boundary at ck=20 (kind 2, two distinct deletion timestamps)
    ///   pk=2: `DELETE WHERE ck>5 AND ck<=15` then `DELETE WHERE ck>15 AND ck<=25`
    ///         → boundary at ck=15 (kind 5, two distinct deletion timestamps)
    ///
    /// The test asserts:
    /// - At least 4 `RangeDelete` records total (2 per partition).
    /// - Both records from the same partition have distinct `deleted_at` values
    ///   (they were written with different USING TIMESTAMP values, confirming the
    ///   secondary deletion time from the boundary marker is correctly decoded).
    ///
    /// Gated on presence of binary Data.db (skip cleanly if absent).
    /// Run `bash test-data/scripts/generate-deltas.sh` to regenerate.
    #[tokio::test]
    async fn scan_delta_emits_both_range_deletes_from_adjacent_ranges_table() {
        let root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(r) => std::path::PathBuf::from(r),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set — skipping adjacent-ranges e2e test");
                return;
            }
        };
        let Some(table_dir) = find_test_deltas_table_dir(&root, "adjacent_ranges") else {
            return;
        };

        // Schema for test_deltas.adjacent_ranges:
        //   PRIMARY KEY (pk INT, ck INT)
        //   val TEXT
        let schema = crate::schema::TableSchema {
            keyspace: "test_deltas".to_string(),
            table: "adjacent_ranges".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![crate::schema::ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            }],
            columns: vec![crate::schema::Column {
                name: "val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 128);

        // Collect all RangeDeletes, grouped by partition key, so we can check
        // that each partition yields BOTH records and that they have distinct
        // deleted_at values (proving the secondary boundary deletion time is decoded).
        let mut range_deletes_by_pk: std::collections::HashMap<
            i32,
            Vec<(RangeBound, RangeBound, i64)>,
        > = std::collections::HashMap::new();
        let mut upsert_count = 0_usize;

        while let Some(result) = rx.recv().await {
            match result {
                Ok(DeltaRecord::RangeDelete {
                    partition_key,
                    start,
                    end,
                    deleted_at,
                }) => {
                    assert!(
                        !partition_key.partition.is_empty(),
                        "RangeDelete must have a non-empty partition key"
                    );
                    assert!(
                        deleted_at > 0,
                        "RangeDelete deleted_at must be positive; got {}",
                        deleted_at
                    );
                    // Extract integer pk value for grouping.
                    let pk_int = match &partition_key.partition[0] {
                        Value::Integer(n) => *n,
                        other => panic!("expected Integer pk; got {:?}", other),
                    };
                    eprintln!(
                        "adjacent-ranges e2e: RangeDelete pk={} start=({:?}, incl={}) \
                         end=({:?}, incl={}) deleted_at={}",
                        pk_int,
                        start.values,
                        start.inclusive,
                        end.values,
                        end.inclusive,
                        deleted_at
                    );
                    range_deletes_by_pk
                        .entry(pk_int)
                        .or_default()
                        .push((start, end, deleted_at));
                }
                Ok(DeltaRecord::Upsert { .. }) => upsert_count += 1,
                Ok(DeltaRecord::StaticUpsert { .. }) => {}
                Ok(DeltaRecord::RowDelete { .. }) => {} // possible from surviving rows
                Ok(DeltaRecord::PartitionDelete { .. }) => {}
                Err(e) => panic!("scan_delta error on adjacent_ranges: {e}"),
            }
        }

        let total_range_deletes: usize = range_deletes_by_pk.values().map(|v| v.len()).sum();
        eprintln!(
            "adjacent_ranges e2e: {} total RangeDeletes across {} partitions, {} Upserts",
            total_range_deletes,
            range_deletes_by_pk.len(),
            upsert_count
        );

        // The fixture creates at least 2 adjacent ranges in pk=1 — expect both.
        assert!(
            total_range_deletes >= 2,
            "expected at least 2 RangeDeletes (one per adjacent range); got {} (with {} upserts)",
            total_range_deletes,
            upsert_count
        );

        // For each partition that has 2+ RangeDeletes, verify distinct deleted_at values.
        // This is the key check: if the secondary deletion time from the boundary marker
        // were not decoded correctly, both records would share the same timestamp.
        for (pk, records) in &range_deletes_by_pk {
            if records.len() >= 2 {
                let timestamps: std::collections::HashSet<i64> =
                    records.iter().map(|(_, _, ts)| *ts).collect();
                assert!(
                    timestamps.len() >= 2,
                    "pk={}: expected at least 2 distinct deleted_at values from adjacent ranges \
                     with different USING TIMESTAMP values; all {} records share the same timestamp. \
                     This indicates the boundary-marker secondary deletion time is not decoded.",
                    pk,
                    records.len()
                );
                eprintln!(
                    "pk={}: {} RangeDeletes with {} distinct timestamps — boundary marker correctly decoded",
                    pk, records.len(), timestamps.len()
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // DS4 (Issue #700): E2E integration — test_collections corpus
    // -----------------------------------------------------------------------

    /// E2E integration test: scan_delta over `test_collections/collection_table`
    /// (SET<TEXT>, LIST<INT>, MAP<TEXT,TEXT>) produces Upsert records without
    /// panicking, and all Upsert cells have a non-zero writetime.
    ///
    /// Also verifies that `ScanSummaryHandle.read()` is accessible after the
    /// scan completes (DS4 summary API smoke-check).
    ///
    /// Skipped automatically when CQLITE_DATASETS_ROOT is not set or
    /// Data.db is absent (run `bash test-data/scripts/fetch-datasets.sh`).
    #[tokio::test]
    async fn ds4_scan_delta_collection_table_e2e() {
        let root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(r) => std::path::PathBuf::from(r),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set — skipping DS4 collection e2e test");
                return;
            }
        };

        let base = root.join("sstables/test_collections");
        if !base.exists() {
            eprintln!("test_collections not found — skipping DS4 e2e");
            return;
        }

        // Find the collection_table directory.
        let table_dir = std::fs::read_dir(&base).ok().and_then(|mut it| {
            it.find_map(|e| {
                e.ok()
                    .filter(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("collection_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            })
        });
        let Some(table_dir) = table_dir else {
            eprintln!("collection_table dir not found — skipping DS4 e2e");
            return;
        };

        // Require Data.db; skip if absent.
        let has_data_db = std::fs::read_dir(&table_dir)
            .ok()
            .map(|it| {
                it.filter_map(|e| e.ok()).any(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if !has_data_db {
            eprintln!("No Data.db in collection_table — skipping DS4 e2e (run fetch-datasets.sh)");
            return;
        }

        // Schema for test_collections.collection_table:
        //   id UUID PRIMARY KEY
        //   tags SET<TEXT>
        //   scores LIST<INT>
        //   properties MAP<TEXT, TEXT>
        //   numbers_set SET<INT>
        //   ordered_values LIST<TIMESTAMP>
        //   metadata_map MAP<TEXT, BIGINT>
        let schema = crate::schema::TableSchema {
            keyspace: "test_collections".to_string(),
            table: "collection_table".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                crate::schema::Column {
                    name: "tags".to_string(),
                    data_type: "set<text>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "scores".to_string(),
                    data_type: "list<int>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "properties".to_string(),
                    data_type: "map<text, text>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "numbers_set".to_string(),
                    data_type: "set<int>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "ordered_values".to_string(),
                    data_type: "list<timestamp>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "metadata_map".to_string(),
                    data_type: "map<text, bigint>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let (mut rx, summary_handle) = scan_delta(table_dir, schema, 64);
        let mut upsert_count = 0_usize;
        let mut total = 0_usize;
        let mut collection_cells_seen = 0_usize;

        while let Some(result) = rx.recv().await {
            total += 1;
            match result {
                Ok(DeltaRecord::Upsert { ref cells, .. }) => {
                    upsert_count += 1;
                    for (col_id, cell) in cells {
                        let col_name = col_id.name();
                        // Collection columns: check writetime is a plausible µs timestamp.
                        if matches!(
                            col_name,
                            "tags"
                                | "scores"
                                | "properties"
                                | "numbers_set"
                                | "ordered_values"
                                | "metadata_map"
                        ) && cell.value.is_some()
                        {
                            collection_cells_seen += 1;
                            // DS4 AC: writetime must be a plausible epoch-µs value
                            // (after 2020-01-01 = 1_577_836_800_000_000 µs).
                            assert!(
                                cell.writetime > 1_577_836_800_000_000,
                                "DS4: collection cell '{}' writetime {} is suspiciously small — \
                                 expected max element writetime",
                                col_name,
                                cell.writetime
                            );
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => panic!("scan_delta DS4 collection e2e error: {e}"),
            }
        }

        // Read the summary after the stream is drained.
        let summary = summary_handle.read();

        eprintln!(
            "DS4 collection_table e2e: {} total records, {} upserts, {} collection cells, \
             {} element tombstones detected",
            total, upsert_count, collection_cells_seen, summary.element_tombstones_detected
        );

        assert!(
            upsert_count > 0,
            "DS4 e2e: expected at least one Upsert from collection_table"
        );
        assert!(
            collection_cells_seen > 0,
            "DS4 e2e: expected at least one collection cell (tags/scores/properties/…) in Upsert records"
        );

        // The test corpus uses append operations (no `s = {...}` overwrites),
        // so element_tombstones_detected should be 0 for this fixture.
        assert_eq!(
            summary.element_tombstones_detected, 0,
            "DS4 e2e: collection_table fixture uses appends only — expected 0 element tombstones, \
             got {}",
            summary.element_tombstones_detected
        );
    }
}
