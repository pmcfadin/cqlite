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
use std::ops::ControlFlow;
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

/// The schema's STATIC column names, or `None` when the static adaptation does not
/// apply at all (issue #3095).
///
/// A static column REQUIRES a clustering column in CQL (a table without one cannot
/// declare a static column), and the clustering requirement is what makes
/// "`clustering_key: None` identifies the static row" sound: with clustering columns
/// present, no ordinary row can have an absent clustering key. Returning `None` here
/// is what keeps every non-static table on its previous code path exactly.
pub(crate) fn static_columns_of(
    schema: &cqlite_core::schema::TableSchema,
) -> Option<Vec<Arc<str>>> {
    if schema.clustering_keys.is_empty() {
        return None;
    }
    let cols: Vec<Arc<str>> = schema
        .columns
        .iter()
        .filter(|c| c.is_static)
        .map(|c| Arc::from(c.name.as_str()))
        .collect();
    (!cols.is_empty()).then_some(cols)
}

/// The per-partition static DECISIONS — the single place Cassandra's
/// `processPartition()` static rules live (issue #3095).
///
/// Shared by BOTH drivers, so the two can never drift apart on the semantics:
/// * the ROW-GRANULAR streaming route ([`StaticMergeSource`], the production
///   `do_get` row path), and
/// * the PARTITION-GRANULAR buffered + AGGREGATE routes
///   ([`drive_partition_rows`], reached by `producer::drive_merge` /
///   `producer::drive_aggregate`).
pub(crate) struct StaticPartitionState {
    /// The schema's STATIC column names (authoritative; never inferred).
    static_columns: Vec<Arc<str>>,
    /// The static values to inject into each of the partition's clustering rows.
    statics: StaticValues,
    /// The partition's materialized static row, held back until the partition ends —
    /// emitted only if the partition yields no visible clustering row.
    static_row: Option<QueryRow>,
    /// Whether a VISIBLE clustering row was produced for this partition.
    ///
    /// "Produced", NOT "survived the request's predicate" — and that is the
    /// Cassandra-faithful choice, not a convenience. A predicate can only remove
    /// clustering rows when it restricts a clustering or REGULAR column, and in
    /// exactly that case `returnStaticContentOnPartitionWithNoRows()` is FALSE
    /// (`queriesFullPartitions()` =
    /// `!hasClusteringColumnsRestrictions() && !hasRegularColumnsRestrictions()`), so
    /// Cassandra returns ZERO rows for the partition. Treating a filtered-out row as
    /// "the partition had rows" therefore yields the same result set, and a bare
    /// `SELECT *` — where the static-only row IS returned — has no predicate to
    /// remove anything.
    emitted_clustering_row: bool,
}

impl StaticPartitionState {
    pub(crate) fn new(static_columns: Vec<Arc<str>>) -> Self {
        Self {
            static_columns,
            statics: Vec::new(),
            static_row: None,
            emitted_clustering_row: false,
        }
    }

    /// Clear all partition-scoped state (called at every partition boundary).
    pub(crate) fn reset(&mut self) {
        self.statics = Vec::new();
        self.static_row = None;
        self.emitted_clustering_row = false;
    }

    /// Record the partition's static row: keep the full row for the
    /// zero-clustering-row case, and its static column values for injection.
    ///
    /// FAILS LOUDLY when the static row arrives AFTER a clustering row of the same
    /// partition (issue #3095 B6). The merger sorts every `clustering_key: None`
    /// entry FIRST within a partition (`write_engine/merge/streaming.rs`'s
    /// `static_row_carrier_always_sorts_first_regardless_of_partition_width`), so
    /// this cannot happen today — but if that invariant ever broke, the rows already
    /// handed downstream would carry a NULL static column and the loss would be
    /// SILENT and total for the partition. An error is the only safe outcome.
    pub(crate) fn record_static_row(
        &mut self,
        key: &DecoratedKey,
        row: QueryRow,
    ) -> Result<(), ProducerError> {
        if self.emitted_clustering_row {
            return Err(ProducerError::Merge(cqlite_core::Error::corruption(
                format!(
                    "static row for partition {:02x?} arrived AFTER a clustering row of \
                 the same partition — the merger's static-sorts-first invariant is \
                 broken, and the clustering rows already emitted carry a NULL static \
                 column (issue #3095)",
                    &key.key[..key.key.len().min(16)]
                ),
            )));
        }
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
        Ok(())
    }

    /// Inject the partition's static values into a clustering row and note that the
    /// partition has produced a visible row.
    ///
    /// Cassandra fills each `case STATIC:` slot from the partition-level
    /// `staticRow`, and a Cassandra-written clustering row NEVER carries a static
    /// cell — the decoder's `merge_static_cells` documents that disjointness as a
    /// construction property (a static cell's column has `is_static == true`; a
    /// clustering row's cells are the clustering pseudo-cells plus the
    /// `is_static == false` columns). On genuine Cassandra bytes, therefore, "fill
    /// the absent column" and "overwrite" are the SAME operation and the two arms
    /// agree exactly.
    ///
    /// The out-of-contract input differs in a way worth naming rather than
    /// papering over: a CQLite-written SSTable that mis-places a static cell into
    /// the clustering row (the write-side #1074 shape) makes the decoder APPEND a
    /// second same-named cell (it performs no membership check), whereas this arm
    /// keeps the clustering row's own value. Both then surface a single value to a
    /// name-keyed consumer; which one is the consumer's keying rule on the decoder
    /// arm. No committed Cassandra fixture exercises it.
    pub(crate) fn inject_into_clustering_row(&mut self, row: &mut QueryRow) {
        for (name, value) in &self.statics {
            row.values
                .entry(Arc::clone(name))
                .or_insert_with(|| value.clone());
        }
        self.emitted_clustering_row = true;
    }

    /// The static-only row this partition owes, if any — consumed, so it can never be
    /// emitted twice.
    ///
    /// Returning `None` because `emitted_clustering_row` is set is the CORRECT
    /// Cassandra outcome, not a loss: with N > 0 clustering rows `processPartition()`
    /// emits N rows and no separate static row, and each of those rows already
    /// carries the static values via [`Self::inject_into_clustering_row`]. The
    /// *ordering* hazard is rejected loudly at record time instead (B6), so it can
    /// never reach here.
    pub(crate) fn take_static_only_row(&mut self) -> Option<QueryRow> {
        let row = self.static_row.take()?;
        (!self.emitted_clustering_row).then_some(row)
    }
}

/// Drive ONE partition's reconciled entries through Cassandra's static semantics,
/// handing each resulting row to `emit` (issue #3095, NB2).
///
/// This is the PARTITION-GRANULAR choke point: `producer::drive_merge` (the buffered
/// collect route behind the public `produce` / `produce_from_paths` /
/// `produce_from_resolved`) and `producer::drive_aggregate` (every aggregating
/// ticket, e.g. `SELECT count(*)`) both step the merge one WHOLE partition at a time
/// and must not each grow their own copy of these rules. Both call this.
///
/// Uniform for static and non-static tables: with no static column the state is
/// `None` and this is exactly the previous "materialize each entry, skip the
/// suppressed ones" loop. Streaming, not buffering — `emit` is called per row, so
/// peak memory is unchanged (no `Vec<QueryRow>` per partition), and `emit` may stop
/// the scan early with [`ControlFlow::Break`] (the `LIMIT` path).
pub(crate) fn drive_partition_rows<F>(
    producer: &MergeProducer,
    key: &DecoratedKey,
    rows: Vec<MergeEntry>,
    pk_cache: &mut PartitionKeyCache,
    assemble_cols: Option<&HashSet<String>>,
    mut emit: F,
) -> Result<ControlFlow<()>, ProducerError>
where
    F: FnMut(QueryRow) -> Result<ControlFlow<()>, ProducerError>,
{
    let mut state = static_columns_of(&producer.schema).map(StaticPartitionState::new);
    for entry in rows {
        let is_static_entry = state.is_some() && entry.clustering_key.is_none();
        let Some(mut row) =
            producer.entry_to_row(&key.key, entry, pk_cache, assemble_cols, producer.now_secs)?
        else {
            continue;
        };
        match (is_static_entry, state.as_mut()) {
            // The reconciled static row: held back, never emitted as its own row
            // unless the partition turns out to have no visible clustering row.
            (true, Some(state)) => state.record_static_row(key, row)?,
            (_, Some(state)) => {
                state.inject_into_clustering_row(&mut row);
                if emit(row)?.is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
            (_, None) => {
                if emit(row)?.is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
        }
    }
    // Cassandra's zero-clustering-row branch: exactly ONE row, statics populated,
    // clustering + regular columns null.
    if let Some(row) = state
        .as_mut()
        .and_then(StaticPartitionState::take_static_only_row)
    {
        if emit(row)?.is_break() {
            return Ok(ControlFlow::Break(()));
        }
    }
    Ok(ControlFlow::Continue(()))
}

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
    /// The per-partition static decisions, SHARED with the partition-granular route
    /// (see [`StaticPartitionState`]) so the semantics exist in exactly one place.
    state: StaticPartitionState,
    pk_cache: PartitionKeyCache,
    assemble_cols: Option<HashSet<String>>,
    /// The partition currently being adapted.
    partition: Option<DecoratedKey>,
    /// A FINAL step (never a `Row`) held back one `next_step` call so a partition's
    /// static-only row can be emitted before it.
    ///
    /// Deliberately separate from [`Self::deferred_input`]: a step parked here needs
    /// NO adaptation, so returning it verbatim is sound. A `Row` must never be
    /// parked here — that was the rust-reviewer BLOCKER (B3): a raw
    /// `PendingRow::Merged` returned straight out of this fast path bypassed
    /// `entry_is_static` / `record_static_row` / `inject` entirely, so the first
    /// entry of the next partition (its STATIC row) was emitted verbatim as the very
    /// phantom `ck = null` row this module exists to remove. The type no longer
    /// permits it: `SourceStep::Row` goes to `deferred_input` and is RE-ADAPTED.
    ready: Option<FinalStep>,
    /// An inner `Row` step deferred by one `next_step` call, re-fed through the FULL
    /// adaptation on the next call (issue #3095 B3).
    deferred_input: Option<(DecoratedKey, PendingRow)>,
    /// Whether the CURRENT partition is excluded by the request's token filter.
    ///
    /// Issue #3095 (B5): the drive loop evaluates the token filter BEFORE
    /// materializing precisely so a token-excluded partition costs no row
    /// construction and a decode error inside one cannot surface for a partition the
    /// split does not own (`row_source::PendingRow`). This adapter materializes
    /// rows itself, so it must apply the SAME filter first — otherwise that
    /// invariant would hold for every table except a static-bearing one.
    token_excluded: bool,
}

/// A step that carries no row and therefore needs no static adaptation, so it can be
/// parked and replayed verbatim (issue #3095 B3).
enum FinalStep {
    PartitionEnd(DecoratedKey),
    Complete,
}

impl From<FinalStep> for SourceStep {
    fn from(step: FinalStep) -> Self {
        match step {
            FinalStep::PartitionEnd(key) => SourceStep::PartitionEnd(key),
            FinalStep::Complete => SourceStep::Complete,
        }
    }
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
        let static_columns = static_columns_of(&producer.schema)?;
        Some(StaticMergeSource {
            inner,
            producer,
            state: StaticPartitionState::new(static_columns),
            pk_cache: PartitionKeyCache::default(),
            assemble_cols: producer.assemble_columns(),
            partition: None,
            ready: None,
            deferred_input: None,
            token_excluded: false,
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
        self.state.reset();
        // Issue #3095 (B5): the SAME token predicate `drive_row_source` applies, run
        // once per partition BEFORE any materialization.
        self.token_excluded = match &self.producer.spec.token {
            Some(token) => !token.contains(key.token),
            None => false,
        };
        owed
    }

    /// The static-only row the CURRENT partition owes, if any, paired with its key.
    /// Delegates the DECISION to the shared [`StaticPartitionState`].
    fn take_static_only_row(&mut self) -> Option<(DecoratedKey, QueryRow)> {
        let row = self.state.take_static_only_row()?;
        let key = self.partition.clone()?;
        Some((key, row))
    }

    /// The materialized-row step for `key`, as the drive loop consumes it.
    fn materialized_step(key: DecoratedKey, row: QueryRow) -> SourceStep {
        SourceStep::Row(key, PendingRow::Materialized(Box::new(row)))
    }
}

impl StaticMergeSource<'_> {
    /// Apply the static adaptation to ONE inner `Row` increment.
    ///
    /// Every `Row` reaching the drive loop passes through here — including one that
    /// a previous call deferred (B3), which is why the deferral parks the RAW input
    /// rather than a finished step.
    fn adapt_row(
        &mut self,
        key: DecoratedKey,
        pending: PendingRow,
    ) -> Result<SourceStep, ProducerError> {
        // A new partition: flush the previous one's static-only row first and defer
        // this input by one call (re-adapted, never returned verbatim).
        if self.partition.as_ref() != Some(&key) {
            if let Some((owed_key, owed_row)) = self.rotate_to(&key) {
                self.deferred_input = Some((key, pending));
                return Ok(Self::materialized_step(owed_key, owed_row));
            }
        }
        // Issue #3095 (B5): a token-excluded partition costs NO row construction —
        // the invariant `drive_row_source` upholds for every other table. The drive
        // loop drops the increment on its own token check; this only makes sure we
        // never materialize (so a decode error inside a partition the split does not
        // own cannot fail this request).
        if self.token_excluded {
            return Ok(SourceStep::Row(key, PendingRow::Suppressed));
        }
        let is_static_entry = matches!(
            &pending,
            PendingRow::Merged(entry) if entry_is_static(entry)
        );
        let Some(mut row) = self.materialize(&key, pending)? else {
            // Suppressed (a tombstone/carrier/expired row). Still handed downstream
            // as a suppressed increment so the drive loop's per-partition accounting
            // is byte-identical to the unadapted source.
            return Ok(SourceStep::Row(key, PendingRow::Suppressed));
        };
        if is_static_entry {
            // The reconciled static row. Held back: it is emitted only if this
            // partition turns out to have no visible clustering row.
            self.state.record_static_row(&key, row)?;
            return Ok(SourceStep::Row(key, PendingRow::Suppressed));
        }
        self.state.inject_into_clustering_row(&mut row);
        Ok(Self::materialized_step(key, row))
    }

    /// Park a FINAL step and emit the current partition's owed static-only row
    /// before it; or return the step unchanged when nothing is owed.
    fn close_with(&mut self, step: FinalStep) -> SourceStep {
        match self.take_static_only_row() {
            Some((owed_key, owed_row)) => {
                self.ready = Some(step);
                Self::materialized_step(owed_key, owed_row)
            }
            None => step.into(),
        }
    }
}

impl RowSource for StaticMergeSource<'_> {
    fn next_step(&mut self) -> Result<SourceStep, ProducerError> {
        // A parked FINAL step needs no adaptation (see `ready`).
        if let Some(step) = self.ready.take() {
            return Ok(step.into());
        }
        // A deferred `Row` is re-fed through the FULL adaptation (B3).
        if let Some((key, pending)) = self.deferred_input.take() {
            return self.adapt_row(key, pending);
        }
        match self.inner.next_step()? {
            SourceStep::Row(key, pending) => self.adapt_row(key, pending),
            // The merger closes EVERY partition with a `PartitionEnd`
            // (`merge/streaming.rs`), so this is the normal flush point for a
            // static-only partition.
            SourceStep::PartitionEnd(key) => Ok(self.close_with(FinalStep::PartitionEnd(key))),
            // Defensive: a source that completes without a final `PartitionEnd` must
            // still not silently drop a row Cassandra returns.
            SourceStep::Complete => Ok(self.close_with(FinalStep::Complete)),
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

#[cfg(test)]
#[path = "statics_tests.rs"]
mod tests;
