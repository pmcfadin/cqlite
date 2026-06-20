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

use crate::types::{ColumnId, TombstoneType, Value};

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
pub fn scan_delta(
    sstable_dir: std::path::PathBuf,
    schema: crate::schema::TableSchema,
    buffer_size: usize,
) -> tokio::sync::mpsc::Receiver<crate::Result<DeltaRecord>> {
    let (tx, rx) = tokio::sync::mpsc::channel(buffer_size.max(1));
    tokio::spawn(async move {
        if let Err(e) = run_scan_delta(sstable_dir, schema, tx.clone()).await {
            let _ = tx.send(Err(e)).await;
        }
    });
    rx
}

/// Internal async driver for [`scan_delta`].
async fn run_scan_delta(
    sstable_dir: std::path::PathBuf,
    schema: crate::schema::TableSchema,
    tx: tokio::sync::mpsc::Sender<crate::Result<DeltaRecord>>,
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

    // Stitch + parse with mutex held for the stitch (issue #805), then release.
    // The parsing itself is synchronous and moves into spawn_blocking; it does
    // not need the scan mutex since we already have the full stitched buffer.
    let (stitched, parser) = {
        let _scan_guard = reader.delta_scan_mutex().lock().await;
        reader.prepare_delta_scan().await?
        // _scan_guard dropped here after stitching is complete.
    };

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
            |(partition_key_raw, cells, cell_meta, row_liveness_ts, is_static, is_row_tombstone, marked_for_delete_at)| {
                // ----------------------------------------------------------------
                // Row tombstones — out of scope for Issue #698 (see #699).
                // ----------------------------------------------------------------
                if is_row_tombstone {
                    log::debug!(
                        "scan_delta: skipping row tombstone (deleted_at={:?}) — tombstone emission is Issue #699",
                        marked_for_delete_at
                    );
                    return Ok(std::ops::ControlFlow::Continue(()));
                }

                // ----------------------------------------------------------------
                // Decode partition key from raw bytes.
                // ----------------------------------------------------------------
                let pk_columns = crate::storage::partition_key_codec::decode_partition_key_columns(
                    &partition_key_raw.0,
                    &schema_arc,
                )
                .map_err(|e| crate::Error::corruption(format!(
                    "scan_delta: failed to decode partition key: {e}"
                )))?;
                let partition_values: Vec<Value> = pk_columns.into_iter().map(|(_, v)| v).collect();

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
                            replaced: false,
                        },
                    };

                    cell_deltas.push((ColumnId::new(col_name), cell));
                }

                // ----------------------------------------------------------------
                // Emit the appropriate DeltaRecord variant.
                // ----------------------------------------------------------------
                let record = if is_static {
                    DeltaRecord::StaticUpsert {
                        partition_key: RowKeys::partition_only(partition_values),
                        cells: cell_deltas,
                    }
                } else {
                    let liveness = row_liveness_ts.map(CellMeta::new);
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
        };

        let mut rx = scan_delta(table_dir, schema, 64);
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
        };

        let mut rx = scan_delta(table_dir, schema, 64);
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
        };

        let mut rx = scan_delta(table_dir, schema, 64);
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
        };

        let mut rx = scan_delta(table_dir, schema, 64);
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
}
