//! Cassandra's STATIC-column `SELECT` semantics for the k-way MERGE arm of the
//! `do_get` row route (issue #3095).
//!
//! # The oracle
//!
//! `cassandra-5.0.8:src/java/org/apache/cassandra/cql3/statements/SelectStatement.java`,
//! `processPartition()`:
//!
//! * `Row staticRow = partition.staticRow()` (L1097) fetches the static row OUT OF
//!   BAND (`db/rows/BaseRowIterator.java` L54-58), so it is never an element of the
//!   iteration and `partition.hasNext()` counts CLUSTERING rows only;
//! * with N > 0 clustering rows (L1122-1152) the loop emits exactly N result rows
//!   and every `case STATIC:` slot is filled from that partition-level `staticRow`
//!   — so the static value appears on EVERY row and NO extra row is produced;
//! * with ZERO clustering rows (L1099-1120) exactly ONE row is emitted, with
//!   clustering and REGULAR columns null, then the method `return`s — making the
//!   two shapes mutually exclusive.
//!
//! # Why this layer exists (and why the two arms differ here)
//!
//! CQLite's k-way merger is a COMPACTION merger: it streams the reconciled static
//! row as an ordinary `MergeEntry` with `clustering_key: None`, sorted first in the
//! partition (`write_engine/merge/streaming.rs`). Handed straight to the row drive
//! loop that becomes a phantom `ck = null` row alongside the real rows, and the
//! clustering rows never receive the static value — precisely the #3095 defect.
//!
//! The single-generation SCAN arm has the opposite input shape: its decoder already
//! injects statics into each clustering row and (since #3095) emits the
//! static-only-partition row itself, so it needs no adaptation. There is therefore
//! no single "both arms look alike" seam to fix — this module converts the MERGE
//! arm's input shape into the same OUTPUT shape the scan arm already produces, and
//! the equality of the two is asserted end-to-end by
//! `cqlite-flight/tests/issue_3095_flight_static_columns.rs` (both arms, same
//! bytes, pinned `now`).
//!
//! Only installed when the schema declares BOTH a static column and a clustering
//! column, so a table without statics keeps the previous code path exactly.

use std::collections::HashSet;
use std::sync::Arc;

use cqlite_core::query::{PartitionKeyCache, QueryRow};
use cqlite_core::storage::write_engine::merge::MergeEntry;
use cqlite_core::storage::write_engine::DecoratedKey;
use cqlite_core::types::Value;

use crate::producer::{MergeProducer, ProducerError};
use crate::row_source::{PendingRow, RowSource, SourceStep};

/// A partition's static column values, as materialized from its reconciled static
/// row (so cell tombstones, TTL expiry and row liveness have ALREADY been applied
/// by `entry_to_row` at the request's `now_secs`).
type StaticValues = Vec<(Arc<str>, Value)>;

/// Adapts the MERGE arm's row stream to Cassandra's static semantics (see the
/// module docs).
///
/// Every row is materialized HERE rather than in the drive loop, because both
/// decisions this layer must make depend on the materialized result:
/// * whether a `clustering_key: None` entry is the partition's static row (it
///   materializes to `Some`) or a range/partition-tombstone carrier (`None`), and
/// * whether the partition yielded a VISIBLE clustering row — Cassandra's
///   `partition.hasNext()` is evaluated over the ALREADY-filtered `RowIterator`
///   (`UnfilteredRowIterators.filter`), so a partition whose every clustering row
///   is deleted/expired counts as having none and does return its static content.
pub(crate) struct StaticMergeSource<'a> {
    inner: &'a mut dyn RowSource,
    producer: &'a MergeProducer,
    /// The schema's STATIC column names (authoritative; never inferred).
    static_columns: Vec<Arc<str>>,
    pk_cache: PartitionKeyCache,
    assemble_cols: Option<HashSet<String>>,
    /// The partition currently being adapted.
    partition: Option<DecoratedKey>,
    /// The partition's materialized static row, held back until the partition ends
    /// — emitted only if the partition yields no visible clustering row.
    static_row: Option<QueryRow>,
    /// The static values to inject into each of the partition's clustering rows.
    statics: StaticValues,
    /// Whether a VISIBLE clustering row was handed downstream for this partition.
    emitted_clustering_row: bool,
    /// A step deferred by one `next_step` call, so a partition's static-only row
    /// can be emitted BEFORE the first step of the next partition.
    queued: Option<SourceStep>,
}

impl<'a> StaticMergeSource<'a> {
    /// Wrap `inner` when — and only when — `producer`'s schema declares both a
    /// static column and a clustering column. Returns `None` otherwise, so a
    /// non-static table keeps the unadapted merge source.
    ///
    /// A static column REQUIRES a clustering column in CQL (a table with no
    /// clustering column cannot declare one), so the clustering check is what makes
    /// "`clustering_key: None` identifies the static row" sound: with clustering
    /// columns present, no ordinary row can have an absent clustering key.
    pub(crate) fn wrap(
        producer: &'a MergeProducer,
        inner: &'a mut dyn RowSource,
    ) -> Option<StaticMergeSource<'a>> {
        let schema = &producer.schema;
        if schema.clustering_keys.is_empty() {
            return None;
        }
        let static_columns: Vec<Arc<str>> = schema
            .columns
            .iter()
            .filter(|c| c.is_static)
            .map(|c| Arc::from(c.name.as_str()))
            .collect();
        if static_columns.is_empty() {
            return None;
        }
        Some(StaticMergeSource {
            inner,
            producer,
            static_columns,
            pk_cache: PartitionKeyCache::default(),
            assemble_cols: producer.assemble_columns(),
            partition: None,
            static_row: None,
            statics: Vec::new(),
            emitted_clustering_row: false,
            queued: None,
        })
    }

    /// Materialize one pending row through the producer's shared row assembly.
    fn materialize(
        &mut self,
        key: &DecoratedKey,
        pending: PendingRow,
    ) -> Result<Option<QueryRow>, ProducerError> {
        self.producer.materialize_pending(
            key,
            pending,
            &mut self.pk_cache,
            self.assemble_cols.as_ref(),
        )
    }

    /// Begin a new partition, returning the PREVIOUS partition's static-only row
    /// when it owes one (no visible clustering row was emitted for it).
    fn rotate_to(&mut self, key: &DecoratedKey) -> Option<(DecoratedKey, QueryRow)> {
        let owed = self.take_static_only_row();
        self.partition = Some(key.clone());
        self.static_row = None;
        self.statics = Vec::new();
        self.emitted_clustering_row = false;
        owed
    }

    /// The static-only row the CURRENT partition owes, if any — consumed, so it can
    /// never be emitted twice.
    fn take_static_only_row(&mut self) -> Option<(DecoratedKey, QueryRow)> {
        let row = self.static_row.take()?;
        if self.emitted_clustering_row {
            return None;
        }
        let key = self.partition.clone()?;
        Some((key, row))
    }

    /// Record the partition's static row: keep the full row for the
    /// zero-clustering-row case, and its static column values for injection.
    fn record_static_row(&mut self, row: QueryRow) {
        self.statics = self
            .static_columns
            .iter()
            .filter_map(|name| {
                row.values
                    .get_key_value(name.as_ref())
                    .map(|(k, v)| (Arc::clone(k), v.clone()))
            })
            .collect();
        self.static_row = Some(row);
    }

    /// Inject the partition's static values into a clustering row.
    ///
    /// Cassandra fills each `case STATIC:` slot from the partition-level
    /// `staticRow`, and a Cassandra-written clustering row never carries a static
    /// cell, so on genuine Cassandra bytes "fill the absent column" and "overwrite"
    /// are the same operation. Filling only absent columns is chosen deliberately:
    /// it matches the single-generation decoder's `merge_static_cells`
    /// (clustering-row-wins), keeping the two arms identical even on a
    /// CQLite-written SSTable that mis-places a static cell into the clustering row
    /// (the write-side #1074 shape).
    fn inject(&self, row: &mut QueryRow) {
        for (name, value) in &self.statics {
            row.values
                .entry(Arc::clone(name))
                .or_insert_with(|| value.clone());
        }
    }

    /// The materialized-row step for `key`, as the drive loop consumes it.
    fn materialized_step(key: DecoratedKey, row: QueryRow) -> SourceStep {
        SourceStep::Row(key, PendingRow::Materialized(Box::new(row)))
    }
}

impl RowSource for StaticMergeSource<'_> {
    fn next_step(&mut self) -> Result<SourceStep, ProducerError> {
        if let Some(step) = self.queued.take() {
            return Ok(step);
        }
        let step = self.inner.next_step()?;
        match step {
            SourceStep::Row(key, pending) => {
                // A new partition: flush the previous one's static-only row first
                // and defer this step by one call.
                if self.partition.as_ref() != Some(&key) {
                    if let Some((owed_key, owed_row)) = self.rotate_to(&key) {
                        self.queued = Some(SourceStep::Row(key, pending));
                        return Ok(Self::materialized_step(owed_key, owed_row));
                    }
                }
                let is_static_entry = matches!(
                    &pending,
                    PendingRow::Merged(entry) if entry_is_static(entry)
                );
                let Some(mut row) = self.materialize(&key, pending)? else {
                    // Suppressed (a tombstone/carrier/expired row). Still handed
                    // downstream as a suppressed increment so the drive loop's
                    // per-partition accounting is byte-identical to the unadapted
                    // source.
                    return Ok(SourceStep::Row(key, PendingRow::Suppressed));
                };
                if is_static_entry {
                    // The reconciled static row. Held back: it is emitted only if
                    // this partition turns out to have no visible clustering row.
                    self.record_static_row(row);
                    return Ok(SourceStep::Row(key, PendingRow::Suppressed));
                }
                self.inject(&mut row);
                self.emitted_clustering_row = true;
                Ok(Self::materialized_step(key, row))
            }
            SourceStep::PartitionEnd(key) => {
                // The merger closes EVERY partition with a `PartitionEnd`, so this
                // is the normal flush point for a static-only partition.
                match self.take_static_only_row() {
                    Some((owed_key, owed_row)) => {
                        self.queued = Some(SourceStep::PartitionEnd(key));
                        Ok(Self::materialized_step(owed_key, owed_row))
                    }
                    None => Ok(SourceStep::PartitionEnd(key)),
                }
            }
            SourceStep::Complete => match self.take_static_only_row() {
                // Defensive: a source that completes without a final
                // `PartitionEnd` must still not silently drop a row Cassandra
                // returns.
                Some((owed_key, owed_row)) => {
                    self.queued = Some(SourceStep::Complete);
                    Ok(Self::materialized_step(owed_key, owed_row))
                }
                None => Ok(SourceStep::Complete),
            },
        }
    }
}

/// Whether a reconciled entry is a candidate for the partition's STATIC row.
///
/// Authoritative structural test (issue #28: no byte-pattern inference): the
/// merger carries `clustering_key: None` for the reconciled static row and for
/// range/partition-tombstone carriers, and — because this adapter is only
/// installed for a schema WITH clustering columns — for nothing else. The two are
/// then separated by materialization: a carrier assembles to no visible row.
fn entry_is_static(entry: &MergeEntry) -> bool {
    entry.clustering_key.is_none()
}
