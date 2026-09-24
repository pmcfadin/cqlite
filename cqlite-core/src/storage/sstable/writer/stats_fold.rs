//! Shared per-mutation statistics fold (issue #1668, stage 5c-iv part 2).
//!
//! [`SSTableWriter::write_partition`] folds every mutation of a partition into
//! `StatisticsMetadata` (min/max timestamp, LDT, TTL, the tombstone-drop-time
//! histogram, the live-LDT sentinel, and the partition-level-deletion flag)
//! BEFORE writing any bytes for that partition. The incremental streaming path
//! (`KWayMerger::merge`, `writer/incremental.rs`) sees mutations one at a time
//! and cannot buffer a whole partition to reproduce that ordering, but the
//! fold itself is a pure function of ONE mutation plus the running
//! accumulator — extracted here so both paths call the exact same logic and
//! can never drift.
//!
//! [`SSTableWriter::write_partition`]: super::SSTableWriter::write_partition

use crate::schema::TableSchema;
use crate::storage::sstable::writer::data_writer::DataWriter;
use crate::storage::sstable::writer::stats_writer::StatisticsMetadata;
use crate::storage::write_engine::mutation::{CellOperation, Mutation};

/// Whether `group` (one or more mutations sharing a clustering key) survives
/// into a real emitted row under `shadow_floor` — the SAME decision
/// [`DataWriter::merge_row_group`] (the sole authority for row shadow-drop,
/// issue #1668) makes when actually writing Data.db.
///
/// Exposed as a thin, side-effect-free pre-check (rather than widening
/// `merge_row_group`'s own `pub(super)` visibility out to
/// `write_engine::merge`) so BOTH the buffered
/// ([`super::SSTableWriter::write_partition`]) and incremental
/// (`compact_sstables`'s streaming merge, `writer/incremental.rs`) paths can
/// gate their STATISTICS fold on the SAME real emission decision (issue
/// #4246): a row fully shadow-dropped from Data.db (e.g. a live write
/// shadowed by a covering range/partition tombstone) must not lower
/// persisted `StatisticsMetadata` minima — Cassandra's `MetadataCollector`
/// only ever observes what `SortedTableWriter` actually emits
/// (`SortedTableWriter.java`/`MetadataCollector.java`, cassandra-5.0.8).
///
/// Scope (issue #4246): this gates CLUSTERING-ROW mutations only. A mutation
/// that also carries a static-column operation is deliberately EXCLUDED from
/// this gate by callers (see their own doc comments) — static-cell shadowing
/// by a partition tombstone is a separate, pre-existing question this fix
/// does not attempt, and skipping such a mutation's fold here could
/// under-count a surviving static write.
pub(crate) fn row_group_survives(
    group: &[&Mutation],
    schema: &TableSchema,
    skip_static_ops: bool,
    shadow_floor: Option<i64>,
) -> bool {
    DataWriter::merge_row_group(group, schema, skip_static_ops, shadow_floor).is_some()
}

/// Like [`row_group_survives`], but ALSO returns the group's resolved shadow
/// boundary (`deletion_ts`, `None` when nothing shadows anything) — the SAME
/// value [`DataWriter::merge_row_group`] shadows individual mutations
/// against internally.
///
/// A row GROUP can survive overall (`Some` — e.g. a newer `DELETE` for the
/// clustering key still produces a row) while containing an OLDER mutation
/// in the SAME group (e.g. an earlier `INSERT` for the same clustering key
/// in one flush batch) that is itself fully shadowed by that later
/// deletion — roborev finding on issue #4246: gating only at the group level
/// still folds that older, non-emitted mutation's timestamp. Callers must
/// additionally check each mutation via
/// `deletion_ts.is_none_or(|dts| mutation.timestamp_micros > dts)` (mirroring
/// `merge_row_group`'s own `mutation_shadowed` test) before folding it.
///
/// Also returns the group's RAW resolved row deletion (`row_deletion`,
/// before combining with `shadow_floor`) — the exact tuple
/// `merge_row_group` emits UNCONDITIONALLY as `RowWrite.row_deletion`
/// whenever `Some` (`data_writer/rows.rs`: `row.row_deletion` is set from
/// `resolve_row_deletion`'s result directly, never re-gated against
/// `deletion_ts`). Callers fold this ONCE per group via
/// [`fold_row_deletion_marker`] — never per-mutation (issue #4246 roborev
/// round-2 finding: a per-mutation `carries_row_deletion` exemption in
/// `fold_row_content_stats` either double-folded the winning deletion
/// (already `mutation_shadowed` under a uniform check, since `deletion_ts`
/// is derived from its own timestamp) or wrongly folded a LOSING `DeleteRow`
/// that `resolve_row_deletion` discarded and Data.db never emits).
pub(crate) fn row_group_survival(
    group: &[&Mutation],
    schema: &TableSchema,
    skip_static_ops: bool,
    shadow_floor: Option<i64>,
) -> (bool, Option<i64>, Option<(i64, i32)>) {
    // A SINGLE `merge_row_group` call (issue #4246 roborev round-3
    // performance finding: this used to ALSO call `resolve_row_deletion`
    // standalone before `merge_row_group`, which re-runs it internally as
    // part of its own full reconciliation — a redundant group scan on the
    // write hot path for every row group, on top of the emitter's own
    // separate `merge_row_group` call to actually build the row). When the
    // group survives, `row.row_deletion` IS the exact tuple
    // `resolve_row_deletion` would have returned (see `merge_row_group`'s
    // own doc comment) — no separate call needed. `combine_deletion_ts` is a
    // cheap `Option` combine, not a group re-scan.
    match DataWriter::merge_row_group(group, schema, skip_static_ops, shadow_floor) {
        Some(row) => {
            let deletion_ts = DataWriter::combine_deletion_ts(row.row_deletion, shadow_floor);
            (true, deletion_ts, row.row_deletion)
        }
        // A group with ANY resolved row deletion always produces at least a
        // tombstone row (`merge_row_group`'s own final "produces no row at
        // all" check is `false` whenever `row_deletion.is_some()`), so
        // `None` here implies `row_deletion` was ALSO `None` — every caller
        // only consumes `deletion_ts`/`row_deletion` inside a `survives`
        // branch, so this is behaviorally identical to the two-call form.
        None => (false, None, None),
    }
}

/// Fold the row GROUP's resolved row-deletion marker (the winning `DeleteRow`
/// / issue #932 decoupled `row_tombstone`) exactly ONCE — the third element
/// of [`row_group_survival`]'s return tuple. This is the ONLY place a row's
/// own deletion marker reaches persisted stats: `fold_row_content_stats` no
/// longer special-cases a deletion-carrying mutation (issue #4246 roborev
/// round-2 finding — see its doc comment), so a group with N mutations
/// contending for the row deletion (losing `DeleteRow`s included) folds the
/// winner's `(timestamp, local_deletion_time)` exactly once, matching what
/// `merge_row_group` actually emits.
pub(crate) fn fold_row_deletion_marker(
    stats: &mut StatisticsMetadata,
    row_deletion: Option<(i64, i32)>,
) {
    if let Some((ts, ldt)) = row_deletion {
        stats.update_timestamp(ts);
        stats.update_local_deletion_time(ldt);
    }
}

/// The exact fold sequence for a row GROUP consisting of a SINGLE mutation —
/// the shape both incremental streaming paths use (`KWayMerger::merge`'s
/// `PartitionEnd` handling and `WriteEngine::maintenance_step`'s buffered
/// `PartitionEnd` drain): by the time either sees it, a compaction/merge
/// "cluster group" is already one fully-reconciled `Mutation` per clustering
/// key (unlike `write_partition`'s buffered multi-mutation-per-group case),
/// so `row_group_survival`'s `group` argument is always a one-element slice.
///
/// Extracted (issue #4246 roborev round-3 finding): the two call sites used
/// to hand-assemble this identical four-step sequence independently, so a
/// future edit to one could silently drift from the other with nothing to
/// catch it — this module's own `#1668` equivalence test
/// (`single_mutation_row_group_fold_...`) now exercises THIS function
/// directly, the same one both production paths call.
pub(crate) fn fold_single_mutation_row_group(
    stats: &mut StatisticsMetadata,
    mutation: &Mutation,
    schema: &TableSchema,
    schema_has_static: bool,
    shadow_floor: Option<i64>,
) {
    let (survives, deletion_ts, row_deletion) =
        row_group_survival(std::slice::from_ref(&mutation), schema, false, shadow_floor);
    fold_row_deletion_marker(stats, row_deletion);
    let carries_static = schema_has_static
        && mutation.operations.iter().any(|op| {
            crate::storage::sstable::writer::data_writer::is_static_operation(op, schema)
        });
    if carries_static {
        // Static-cell shadowing uses a SEPARATE, partition-floor-only
        // mechanism unrelated to this row-level `deletion_ts` (out of this
        // fix's verified scope, see `row_group_survives`'s doc comment) —
        // pass `None` so per-op shadow gating never applies to it, exactly
        // the prior unconditional-fold behavior.
        fold_row_content_stats(stats, mutation, None);
    } else if survives {
        fold_row_content_stats(stats, mutation, deletion_ts);
    }
}

/// Fold ONLY the partition/range tombstone MARKER fields of `mutation` (issue
/// #4246 roborev finding). A tombstone marker is never itself row-shadowed —
/// it IS the deletion — so it always contributes, independent of whatever
/// `fold_row_content_stats` decides about the row content sharing the same
/// mutation object. Split out from [`fold_mutation_stats`] so a caller that
/// folds markers unconditionally (once per mutation, regardless of
/// [`row_group_survives`]/[`row_group_survival`]) and row content
/// conditionally (only for surviving mutations) never double-folds a
/// tombstone's local-deletion-time into the tombstone-drop-time histogram —
/// `StatisticsMetadata::update_local_deletion_time` is NOT idempotent (it
/// increments a histogram bucket), so folding the same marker twice inflates
/// the persisted `estimatedTombstoneDropTime` Cassandra derives compaction
/// scheduling from.
pub(crate) fn fold_marker_stats(stats: &mut StatisticsMetadata, mutation: &Mutation) {
    if let Some(pt) = &mutation.partition_tombstone {
        stats.update_timestamp(pt.deletion_time);
        stats.update_local_deletion_time(pt.local_deletion_time);
        stats.mark_partition_level_deletion();
    }
    for rt in &mutation.range_tombstones {
        stats.update_timestamp(rt.deletion_time);
        stats.update_local_deletion_time(rt.local_deletion_time);
    }
}

/// Fold one mutation's timestamp/TTL/local-deletion-time/tombstone information
/// into `stats`. Mirrors the per-mutation loop body that used to live inline in
/// [`super::SSTableWriter::write_partition`] verbatim — same chokepoints
/// (`update_timestamp`, `update_local_deletion_time`, `update_ttl`,
/// `note_live_local_deletion_time`, `mark_partition_level_deletion`), same
/// per-`CellOperation` handling, so folding every mutation of a partition
/// through this function (in any order — the chokepoints are commutative
/// min/max folds, issue #1668 stage 5a) reproduces `write_partition`'s final
/// `StatisticsMetadata` byte-for-byte.
///
/// `#[cfg(test)]` (issue #4246 roborev round 2): every production caller now
/// needs shadow-aware row content
/// (`fold_row_content_stats(stats, mutation, shadow_boundary)`), markers
/// folded separately from row content (`fold_marker_stats`, so a mutation
/// carrying both never double-counts a tombstone into the drop-time
/// histogram), and a row's own deletion folded once at the GROUP level
/// (`fold_row_deletion_marker`, never per-mutation) — this convenience
/// "fold everything for one mutation treated as its own trivial group"
/// wrapper has no remaining non-test caller. A TEST-ONLY CONVENIENCE
/// WRAPPER, not a regression proof (roborev round-2 finding: the earlier
/// wording claimed it reproduces "exactly what folding a mutation
/// unconditionally always did", which is circular once its own definition
/// IS that composition — treats `mutation` as a one-element group via the
/// real `DataWriter::resolve_row_deletion`/`fold_row_deletion_marker` path
/// rather than duplicating that logic, so a future drift between the two
/// would still surface here).
#[cfg(test)]
pub(crate) fn fold_mutation_stats(stats: &mut StatisticsMetadata, mutation: &Mutation) {
    fold_row_content_stats(stats, mutation, None);
    fold_marker_stats(stats, mutation);
    let row_deletion = DataWriter::resolve_row_deletion(&[mutation], None);
    fold_row_deletion_marker(stats, row_deletion);
}

/// Everything [`fold_mutation_stats`] folds EXCEPT the partition/range
/// tombstone marker fields (see [`fold_marker_stats`]'s doc for why the split
/// exists, issue #4246 roborev finding). Used by a caller that folds markers
/// separately/unconditionally so a mutation carrying both row content and a
/// tombstone is never double-counted into the tombstone-drop-time histogram.
///
/// `shadow_boundary` is the row GROUP's resolved `deletion_ts` (see
/// [`row_group_survival`]), or `None` when the caller does not need
/// per-mutation shadow awareness (every existing caller before issue #4246,
/// and the wholly-static/carries-static callers today, which pass `None` to
/// preserve their prior unconditional-fold behavior exactly).
///
/// When `Some(dts)`, the mutation's SIMPLE content (its own leading
/// timestamp, per-cell `cell_write_timestamps`, row-level TTL, and
/// `WriteWithTtl`/`Delete`/`DeleteRow`/`Write` contributions) is
/// shadow-gated on `mutation.timestamp_micros <= dts` — matching
/// `merge_row_group`'s own `mutation_shadowed` test EXACTLY, with NO
/// exemption for a mutation that itself carries a `DeleteRow` op or a
/// `#932` `row_tombstone` (issue #4246 roborev round-2 finding: an earlier
/// version exempted such a mutation, reasoning that "`dts` is derived from
/// its own timestamp when it is the winning deletion" — but `dts` is
/// ALWAYS `>=` any row-deletion-carrying mutation's own timestamp in the
/// group by construction, whether it WON or LOST that contention, so the
/// exemption let a LOSING `DeleteRow`'s never-emitted timestamp/LDT lower
/// persisted minima — precisely the #4246 defect this fix exists to close).
/// This makes the `Delete { .. } | DeleteRow` match arm below structurally
/// unreachable for the `DeleteRow` case specifically (it is always
/// `mutation_shadowed`) — correct, since the group's row deletion is now
/// folded exactly once at the GROUP level via
/// [`fold_row_deletion_marker`]/[`row_group_survival`], never here.
///
/// `ComplexDeletion`/`WriteComplexElement` ops are gated PER-OP instead,
/// each against its OWN independent timestamp (`marked_for_delete_at` /
/// `timestamp_micros`) rather than the mutation's row timestamp — mirroring
/// `merge_row_group`'s rescue branch (`data_writer/rows.rs`'s
/// `mutation_shadowed` handling), because such an op can independently
/// survive and be EMITTED even when the rest of a shadowed mutation is dead
/// (issue #887/#921's whole point, and issue #4246 roborev finding: a
/// per-mutation gate that skips these too under-counts an actually-emitted
/// value).
pub(crate) fn fold_row_content_stats(
    stats: &mut StatisticsMetadata,
    mutation: &Mutation,
    shadow_boundary: Option<i64>,
) {
    let mutation_shadowed = shadow_boundary.is_some_and(|dts| mutation.timestamp_micros <= dts);

    if !mutation_shadowed {
        stats.update_timestamp(mutation.timestamp_micros);
        // Issue #1018: simple `Write`/`WriteWithTtl`/`Delete` cells may carry
        // their OWN (lower) per-cell timestamps in
        // `Mutation::cell_write_timestamps` (a live cell's writetime OR a
        // cell tombstone's markedForDeleteAt) and are emitted with an
        // explicit `min_timestamp` delta. Fold every per-cell timestamp into
        // the stats BEFORE emitting cells so `min_timestamp` can never
        // exceed an emitted cell's actual timestamp (which would underflow
        // the unsigned-VInt delta). Mirrors the pre-pass fold in
        // `compute_mutations_baseline_stats`.
        if let Some(cell_ts) = &mutation.cell_write_timestamps {
            for ts in cell_ts.values() {
                stats.update_timestamp(*ts);
            }
        }
        if let Some(ttl) = mutation.ttl_seconds {
            stats.update_ttl(ttl as i32);
            let now_seconds = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i32)
                .unwrap_or(0);
            let local_deletion_time = now_seconds.saturating_add(ttl as i32);
            stats.update_local_deletion_time(local_deletion_time);
        }
    }
    // Track local deletion times for tombstones and TTL cells. Issue #764:
    // row/cell tombstones use the caller-supplied `local_deletion_time` when
    // present, else the timestamp-derived value.
    for op in &mutation.operations {
        match op {
            CellOperation::WriteWithTtl {
                ttl_seconds,
                local_deletion_time,
                ..
            } => {
                if mutation_shadowed {
                    continue;
                }
                stats.update_ttl(*ttl_seconds as i32);
                // Issue #1538: honor the authoritative per-cell LDT VERBATIM
                // when present (a surviving expiring cell preserved through
                // compaction); `None` keeps the historical `now + ttl`
                // derivation.
                let local_deletion_time = match local_deletion_time {
                    Some(ldt) => *ldt,
                    None => {
                        let now_seconds = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i32)
                            .unwrap_or(0);
                        now_seconds.saturating_add(*ttl_seconds as i32)
                    }
                };
                stats.update_local_deletion_time(local_deletion_time);
            }
            op @ CellOperation::Delete { .. } => {
                if mutation_shadowed {
                    continue;
                }
                // Issue #764 / #921 finding 2: record the EXACT LDT the
                // tombstone is emitted with, via the same helper the emit path
                // uses, so stats and Data.db bytes agree exactly.
                let local_deletion_time =
                    crate::storage::sstable::writer::data_writer::op_cell_local_deletion_time(
                        op, mutation,
                    );
                stats.update_local_deletion_time(local_deletion_time);
            }
            // `DeleteRow` is NEVER folded here (issue #4246 roborev round-3
            // finding): the row's own deletion — whether contributed by a
            // `DeleteRow` op or a #932 decoupled `row_tombstone` — is folded
            // EXACTLY ONCE at the GROUP level via `fold_row_deletion_marker`
            // (see `row_group_survival`'s doc comment), regardless of
            // `shadow_boundary`. This arm used to double-fold the LDT into
            // the tombstone-drop-time histogram specifically for a
            // `carries_static` mutation (whose caller always passes
            // `shadow_boundary = None`, so `mutation_shadowed` was always
            // `false` here and this body unconditionally re-ran after the
            // group-level fold already counted it once) —
            // `update_local_deletion_time` increments a histogram bucket and
            // is NOT idempotent, exactly the defect class
            // `mixed_row_and_partition_tombstone_mutation_folds_tombstone_once`
            // guards markers against.
            CellOperation::DeleteRow => {}
            // Issue #887: a `ComplexDeletion` marker is physically written with
            // its OWN `marked_for_delete_at` / `local_deletion_time`, which may
            // fall outside the row's own timestamp/LDT range. INDEPENDENT of
            // `mutation_shadowed` (issue #4246 roborev finding): gated on its
            // OWN mfda against `shadow_boundary`, mirroring
            // `merge_row_group`'s rescue branch — this marker can survive and
            // be emitted even when the rest of a shadowed mutation is dead.
            CellOperation::ComplexDeletion {
                marked_for_delete_at,
                local_deletion_time,
                ..
            } => {
                if shadow_boundary.is_some_and(|dts| *marked_for_delete_at <= dts) {
                    continue;
                }
                stats.update_timestamp(*marked_for_delete_at);
                stats.update_local_deletion_time(*local_deletion_time);
            }
            // Issue #887: a per-element complex cell carries its OWN explicit
            // timestamp/ttl/local_deletion_time — INDEPENDENT of
            // `mutation_shadowed` for the same reason as `ComplexDeletion`
            // above (issue #4246 roborev finding).
            CellOperation::WriteComplexElement {
                timestamp_micros,
                ttl_seconds,
                local_deletion_time,
                is_deleted,
                ..
            } => {
                if shadow_boundary.is_some_and(|dts| *timestamp_micros <= dts) {
                    continue;
                }
                stats.update_timestamp(*timestamp_micros);
                if let Some(ttl) = ttl_seconds {
                    stats.update_ttl(*ttl as i32);
                }
                if let Some(ldt) = local_deletion_time {
                    stats.update_local_deletion_time(*ldt);
                }
                // Issue #1728 (roborev finding 2): a LIVE complex element
                // carries Cassandra's `NO_DELETION_TIME` sentinel just like a
                // live simple `Write`.
                if !*is_deleted && ttl_seconds.is_none() && local_deletion_time.is_none() {
                    stats.note_live_local_deletion_time();
                }
            }
            // Issue #1728: a live, non-TTL `Write` cell carries Cassandra's
            // `Cell.NO_DELETION_TIME` sentinel as its localDeletionTime.
            CellOperation::Write { value, .. } => {
                if mutation_shadowed {
                    continue;
                }
                if mutation.ttl_seconds.is_none() && !matches!(value, crate::types::Value::Null) {
                    stats.note_live_local_deletion_time();
                }
            }
        }
    }
    // Issue #1721 / #932: a decoupled row tombstone
    // (`Mutation::row_tombstone = Some((deletion_time, ldt))`) is NOT folded
    // here (issue #4246 roborev round-2 finding — removed the earlier
    // unconditional fold). `resolve_row_deletion` already considers
    // `mutation.row_tombstone` as a candidate for the group's winning row
    // deletion, on equal footing with a `DeleteRow` op, so folding it again
    // here would either double-count the winner or wrongly count a losing
    // `row_tombstone` Data.db never emits. The group's actual winning
    // `(deletion_time, ldt)` — from a `DeleteRow` op OR a `row_tombstone`,
    // whichever `resolve_row_deletion` picked — is folded exactly once at
    // the GROUP level via [`fold_row_deletion_marker`].
}

/// Fold `from`'s accumulated range/flags into `into` (issue #1668 stage
/// 5c-iv part 2). Used to merge a partition-scoped fold (accumulated while
/// streaming a partition's mutations one at a time) into the SSTable-wide
/// running `StatisticsMetadata` at partition end, once the incremental
/// session's exclusive borrow of the writer has ended.
///
/// Min/max fields are commutative folds (stage 5a), so feeding both of
/// `from`'s min and max back through the same chokepoints on `into`
/// reproduces exactly what folding every mutation directly into `into` would
/// have produced — PROVIDED `from`'s own untouched-default sentinels are
/// excluded first. `update_timestamp` already self-filters its untouched
/// defaults (`i64::MAX`/`i64::MIN`, both treated as the LIVE/NO_DELETION
/// marker, issue #851), so timestamps are safe to feed unconditionally. LDT
/// and TTL are NOT symmetric:
///   * `update_local_deletion_time` filters `i32::MAX` (live sentinel) but
///     NOT `i32::MIN` (`from.max_local_deletion_time`'s untouched default
///     when no tombstone was ever folded) — guarded explicitly below, else an
///     empty `from` would drag `into.min_local_deletion_time` down to
///     `i32::MIN`.
///   * `update_ttl` only filters non-positive values, so `from.min_ttl`'s
///     untouched default (`i32::MAX`, itself positive) must be excluded
///     explicitly, else it would corrupt `into.max_ttl` to `i32::MAX`.
///
/// The live-LDT sentinel itself is NOT re-derived through
/// `update_local_deletion_time` (which filters `i32::MAX` OUT) —
/// `from.max_local_deletion_time == i32::MAX` is used as the equivalent "saw
/// a live cell" signal instead, since `note_live_local_deletion_time` is the
/// ONLY setter that can assign `i32::MAX` to `max_local_deletion_time` (a
/// real tombstone LDT is always filtered before it can reach the max).
pub(crate) fn merge_stats_fold(into: &mut StatisticsMetadata, from: &StatisticsMetadata) {
    into.update_timestamp(from.min_timestamp);
    into.update_timestamp(from.max_timestamp);

    into.update_local_deletion_time(from.min_local_deletion_time);
    if from.max_local_deletion_time > i32::MIN {
        into.update_local_deletion_time(from.max_local_deletion_time);
    }
    if from.max_local_deletion_time == i32::MAX {
        into.note_live_local_deletion_time();
    }

    if from.min_ttl != i32::MAX {
        into.update_ttl(from.min_ttl);
    }
    into.update_ttl(from.max_ttl);

    if from.has_partition_level_deletions {
        into.mark_partition_level_deletion();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::write_engine::mutation::{
        ClusteringBound, ClusteringKey, PartitionKey, PartitionTombstone, RangeTombstone, TableId,
    };
    use crate::types::Value;

    fn table() -> TableId {
        TableId::new("ks", "t")
    }

    fn pk() -> PartitionKey {
        PartitionKey::single("id", Value::Integer(1))
    }

    fn ck(n: i32) -> ClusteringKey {
        ClusteringKey::single("ck", Value::Integer(n))
    }

    /// A representative set of mutations exercising every branch
    /// `fold_mutation_stats` handles: a partition tombstone, a range
    /// tombstone, a decoupled row tombstone, a TTL write, a `ComplexDeletion`
    /// marker, both a deleted and a LIVE `WriteComplexElement`, a plain live
    /// `Write` (which lifts the live-LDT sentinel), and a null `Write` (which
    /// must NOT).
    fn representative_mutations() -> Vec<Mutation> {
        let mut partition_only = Mutation::new(table(), pk(), None, vec![], 500, None);
        partition_only.partition_tombstone = Some(PartitionTombstone {
            deletion_time: 100,
            local_deletion_time: 1_000,
        });

        let mut range_only = Mutation::new(table(), pk(), None, vec![], 500, None);
        range_only.range_tombstones.push(RangeTombstone {
            start: ClusteringBound::Inclusive(ck(1)),
            end: ClusteringBound::Inclusive(ck(3)),
            deletion_time: 200,
            local_deletion_time: 2_000,
        });

        let row_tombstone_row = Mutation::new(table(), pk(), Some(ck(1)), vec![], 300, None)
            .with_row_tombstone(50, 3_000);

        let ttl_row = Mutation::new(
            table(),
            pk(),
            Some(ck(2)),
            vec![CellOperation::WriteWithTtl {
                column: "v".to_string(),
                value: Value::text("x".to_string()),
                ttl_seconds: 60,
                local_deletion_time: Some(4_000),
            }],
            400,
            Some(60),
        );

        let complex_deletion_row = Mutation::new(
            table(),
            pk(),
            Some(ck(3)),
            vec![CellOperation::ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: 9_000_000,
                local_deletion_time: 5_000,
            }],
            600,
            None,
        );

        let complex_element_row = Mutation::new(
            table(),
            pk(),
            Some(ck(4)),
            vec![
                CellOperation::WriteComplexElement {
                    column: "tags".to_string(),
                    cell_path: b"a".to_vec(),
                    value: None,
                    timestamp_micros: 700,
                    ttl_seconds: None,
                    local_deletion_time: Some(6_000),
                    is_deleted: true,
                },
                CellOperation::WriteComplexElement {
                    column: "tags".to_string(),
                    cell_path: b"b".to_vec(),
                    value: Some(Value::text("b".to_string())),
                    timestamp_micros: 750,
                    ttl_seconds: None,
                    local_deletion_time: None,
                    is_deleted: false,
                },
            ],
            700,
            None,
        );

        let live_write_row = Mutation::new(
            table(),
            pk(),
            Some(ck(5)),
            vec![CellOperation::Write {
                column: "v".to_string(),
                value: Value::text("live".to_string()),
            }],
            800,
            None,
        );

        let null_write_row = Mutation::new(
            table(),
            pk(),
            Some(ck(6)),
            vec![CellOperation::Write {
                column: "v".to_string(),
                value: Value::Null,
            }],
            900,
            None,
        );

        vec![
            partition_only,
            range_only,
            row_tombstone_row,
            ttl_row,
            complex_deletion_row,
            complex_element_row,
            live_write_row,
            null_write_row,
        ]
    }

    /// The relevant `StatisticsMetadata` fields `fold_mutation_stats`/
    /// `merge_stats_fold` actually touch, for equivalence comparison (no
    /// `PartialEq` on the full struct — the histogram/key-range/repair fields
    /// are untouched by this fold and irrelevant here).
    #[derive(Debug, PartialEq)]
    struct FoldSnapshot {
        min_timestamp: i64,
        max_timestamp: i64,
        min_local_deletion_time: i32,
        max_local_deletion_time: i32,
        min_ttl: i32,
        max_ttl: i32,
        has_partition_level_deletions: bool,
    }

    impl From<&StatisticsMetadata> for FoldSnapshot {
        fn from(s: &StatisticsMetadata) -> Self {
            Self {
                min_timestamp: s.min_timestamp,
                max_timestamp: s.max_timestamp,
                min_local_deletion_time: s.min_local_deletion_time,
                max_local_deletion_time: s.max_local_deletion_time,
                min_ttl: s.min_ttl,
                max_ttl: s.max_ttl,
                has_partition_level_deletions: s.has_partition_level_deletions,
            }
        }
    }

    /// Correctness proof (issue #1668 stage 5c-iv part 2): folding every
    /// mutation of a partition DIRECTLY into one accumulator (what
    /// `write_partition` does — the old behavior) must produce the IDENTICAL
    /// final min/max/flag aggregates as the incremental path's split —
    /// folding disjoint SUBSETS of the same mutations into SEPARATE
    /// partition-scoped accumulators (simulating streaming them across
    /// several `feed_row`/`feed_static_row` calls) and then merging those
    /// sub-folds back together via `merge_stats_fold` (simulating
    /// `complete_partition_incremental`).
    #[test]
    fn split_and_merge_matches_direct_fold_for_every_mutation_kind() {
        let mutations = representative_mutations();

        // "write_partition"-equivalent: fold every mutation directly into one
        // running accumulator.
        let mut direct = StatisticsMetadata::new();
        for m in &mutations {
            fold_mutation_stats(&mut direct, m);
        }

        // Incremental-equivalent: split into three arbitrary, non-trivial
        // partition-scoped sub-folds (as if fed across three separate
        // `feed_row`/`feed_static_row` calls before merging at partition
        // end), then merge them all into a fresh running accumulator.
        let mut part_a = StatisticsMetadata::new();
        let mut part_b = StatisticsMetadata::new();
        let mut part_c = StatisticsMetadata::new();
        for (i, m) in mutations.iter().enumerate() {
            match i % 3 {
                0 => fold_mutation_stats(&mut part_a, m),
                1 => fold_mutation_stats(&mut part_b, m),
                _ => fold_mutation_stats(&mut part_c, m),
            }
        }
        let mut merged = StatisticsMetadata::new();
        merge_stats_fold(&mut merged, &part_a);
        merge_stats_fold(&mut merged, &part_b);
        merge_stats_fold(&mut merged, &part_c);

        assert_eq!(
            FoldSnapshot::from(&direct),
            FoldSnapshot::from(&merged),
            "splitting the fold across partition-scoped sub-accumulators and \
             merging must reproduce write_partition's direct fold exactly"
        );

        // Sanity: the live sentinel and partition-deletion flag must actually
        // have been exercised by this fixture, else the equality above would
        // pass vacuously without proving the sentinel-handling guards work.
        assert_eq!(
            direct.max_local_deletion_time,
            i32::MAX,
            "live_write_row must have lifted the live-LDT sentinel"
        );
        assert!(
            direct.has_partition_level_deletions,
            "partition_only must have set the partition-level-deletion flag"
        );
    }

    /// Guard: an EMPTY partition-scoped sub-fold (a partition with no
    /// tombstones/TTLs/live cells reaching this fold at all) must merge as a
    /// true no-op — proving the untouched-default-sentinel guards in
    /// `merge_stats_fold` (`max_local_deletion_time > i32::MIN`, `min_ttl !=
    /// i32::MAX`) actually prevent corruption, not just coincidentally pass
    /// on the representative fixture above.
    #[test]
    fn merging_an_empty_sub_fold_is_a_no_op() {
        let mut into = StatisticsMetadata::new();
        fold_mutation_stats(
            &mut into,
            &Mutation::new(
                table(),
                pk(),
                Some(ck(1)),
                vec![CellOperation::WriteWithTtl {
                    column: "v".to_string(),
                    value: Value::text("x".to_string()),
                    ttl_seconds: 60,
                    local_deletion_time: Some(1_000),
                }],
                100,
                Some(60),
            ),
        );
        let before = FoldSnapshot::from(&into);

        let empty = StatisticsMetadata::new();
        merge_stats_fold(&mut into, &empty);

        assert_eq!(
            FoldSnapshot::from(&into),
            before,
            "merging a never-folded (default) StatisticsMetadata must not \
             change min_local_deletion_time to i32::MIN or max_ttl to i32::MAX"
        );
    }

    /// Direct unit coverage of `fold_row_content_stats(Some(dts))`,
    /// `row_group_survival`, and `fold_row_deletion_marker` in isolation
    /// (issue #4246 roborev round-2 finding: the tests above only exercise
    /// `shadow_boundary = None` via `fold_mutation_stats`, so drift in the
    /// `Some(dts)` gating logic was invisible to this module's own suite).
    mod shadow_gating {
        use super::*;
        use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};

        fn schema() -> TableSchema {
            TableSchema {
                keyspace: "ks".to_string(),
                table: "t".to_string(),
                partition_keys: vec![KeyColumn {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    position: 0,
                }],
                clustering_keys: vec![ClusteringColumn {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    position: 0,
                    order: ClusteringOrder::Asc,
                }],
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        data_type: "int".to_string(),
                        nullable: false,
                        default: None,
                        is_static: false,
                    },
                    Column {
                        name: "ck".to_string(),
                        data_type: "int".to_string(),
                        nullable: false,
                        default: None,
                        is_static: false,
                    },
                    Column {
                        name: "v".to_string(),
                        data_type: "text".to_string(),
                        nullable: true,
                        default: None,
                        is_static: false,
                    },
                ],
                comments: Default::default(),
                dropped_columns: Default::default(),
            }
        }

        fn insert(ck_val: i32, ts: i64) -> Mutation {
            Mutation::new(
                table(),
                pk(),
                Some(ck(ck_val)),
                vec![CellOperation::Write {
                    column: "v".to_string(),
                    value: Value::text("x".to_string()),
                }],
                ts,
                None,
            )
        }

        fn delete_row(ck_val: i32, ts: i64) -> Mutation {
            Mutation::new(
                table(),
                pk(),
                Some(ck(ck_val)),
                vec![CellOperation::DeleteRow],
                ts,
                None,
            )
        }

        /// A mutation fully below `shadow_boundary` folds nothing.
        #[test]
        fn fold_row_content_stats_below_boundary_folds_nothing() {
            let mut stats = StatisticsMetadata::new();
            fold_row_content_stats(&mut stats, &insert(1, 5), Some(10));
            assert_eq!(
                stats.min_timestamp,
                i64::MAX,
                "a mutation at ts=5 shadowed by dts=10 must fold nothing"
            );
        }

        /// A mutation strictly above `shadow_boundary` folds normally.
        #[test]
        fn fold_row_content_stats_above_boundary_folds_everything() {
            let mut stats = StatisticsMetadata::new();
            fold_row_content_stats(&mut stats, &insert(1, 15), Some(10));
            assert_eq!(stats.min_timestamp, 15);
        }

        /// A shadowed mutation's `ComplexDeletion` still folds when its OWN
        /// `marked_for_delete_at` strictly exceeds `dts` (issue #887/#921's
        /// per-op independence).
        #[test]
        fn fold_row_content_stats_shadowed_mutation_complex_deletion_survives_own_timestamp() {
            let mutation = Mutation::new(
                table(),
                pk(),
                Some(ck(1)),
                vec![CellOperation::ComplexDeletion {
                    column: "tags".to_string(),
                    marked_for_delete_at: 20,
                    local_deletion_time: 2_000,
                }],
                5, // row timestamp is itself shadowed
                None,
            );
            let mut stats = StatisticsMetadata::new();
            fold_row_content_stats(&mut stats, &mutation, Some(10));
            assert_eq!(
                stats.min_timestamp, 20,
                "the marker's own mfda=20 exceeds dts=10, so it must survive \
                 even though the carrying mutation's row ts=5 is shadowed"
            );
        }

        /// The exact roborev round-2 finding #1 scenario: two `DeleteRow`s
        /// for the same clustering key in one group, `DeleteRow@5` and
        /// `DeleteRow@20`. `resolve_row_deletion` picks 20 (the winner);
        /// only it may ever reach persisted stats. The losing `DeleteRow@5`
        /// must fold NOTHING via `fold_row_content_stats` (no
        /// `carries_row_deletion` self-exemption), and the winning marker is
        /// folded exactly once via `fold_row_deletion_marker`, not per-mutation.
        #[test]
        fn losing_delete_row_in_same_group_does_not_lower_persisted_minimum() {
            let losing = delete_row(1, 5);
            let winning = delete_row(1, 20);
            let group: Vec<&Mutation> = vec![&losing, &winning];

            let (survives, deletion_ts, row_deletion) =
                row_group_survival(&group, &schema(), false, None);
            assert!(
                survives,
                "a DeleteRow group still produces a row (the tombstone)"
            );
            assert_eq!(
                deletion_ts,
                Some(20),
                "the winning DeleteRow's ts resolves the boundary"
            );
            assert_eq!(
                row_deletion.map(|(ts, _)| ts),
                Some(20),
                "resolve_row_deletion must pick the NEWER DeleteRow, not the older one"
            );

            let mut stats = StatisticsMetadata::new();
            for m in &group {
                fold_row_content_stats(&mut stats, m, deletion_ts);
            }
            assert_eq!(
                stats.min_timestamp,
                i64::MAX,
                "fold_row_content_stats must fold NOTHING for either DeleteRow \
                 mutation — both are `mutation_shadowed` (ts <= dts=20) under the \
                 uniform check, with no self-exemption for the winner"
            );

            fold_row_deletion_marker(&mut stats, row_deletion);
            assert_eq!(
                stats.min_timestamp, 20,
                "the group's winning row-deletion marker (ts=20), folded exactly \
                 once, is the ONLY way this group's timestamp reaches stats — the \
                 losing DeleteRow@5 must never lower the persisted minimum"
            );
        }

        /// `fold_marker_stats` folds a partition/range tombstone
        /// unconditionally, independent of any `shadow_boundary` concept (it
        /// takes no such parameter) — a tombstone marker is never itself
        /// row-shadowed.
        #[test]
        fn fold_marker_stats_folds_partition_tombstone_unconditionally() {
            let mut mutation = Mutation::new(table(), pk(), None, vec![], 999, None);
            mutation.partition_tombstone = Some(PartitionTombstone {
                deletion_time: 42,
                local_deletion_time: 4_242,
            });
            let mut stats = StatisticsMetadata::new();
            fold_marker_stats(&mut stats, &mutation);
            assert_eq!(stats.min_timestamp, 42);
            assert!(stats.has_partition_level_deletions);
        }

        /// Roborev round-3 finding #3: the `carries_static` production
        /// callers always pass `shadow_boundary = None` to
        /// `fold_row_content_stats` (static-cell shadowing uses a separate
        /// mechanism), which used to make `mutation_shadowed` unconditionally
        /// `false` — so a `DeleteRow`-carrying mutation on that path
        /// double-folded its LDT into the tombstone-drop-time histogram: once
        /// via the group-level `fold_row_deletion_marker`, and again via
        /// `fold_row_content_stats`'s own (now-removed) `DeleteRow` handling.
        /// Reproduces the exact `carries_static` call shape
        /// (`fold_row_deletion_marker` once, then
        /// `fold_row_content_stats(mutation, None)`) and asserts the
        /// histogram observes the tombstone exactly once.
        #[test]
        fn delete_row_on_carries_static_path_folds_ldt_exactly_once() {
            let mutation = delete_row(1, 42);
            let group: Vec<&Mutation> = vec![&mutation];
            let (survives, _deletion_ts, row_deletion) =
                row_group_survival(&group, &schema(), false, None);
            assert!(
                survives,
                "a lone DeleteRow group still produces a tombstone row"
            );

            let mut stats = StatisticsMetadata::new();
            // Exactly the `carries_static` branch's call shape: the group's
            // marker folded once, then per-mutation content folded with
            // `shadow_boundary = None` (static-cell shadowing is a separate,
            // out-of-scope mechanism — see the `carries_static` call sites'
            // own doc comments).
            fold_row_deletion_marker(&mut stats, row_deletion);
            fold_row_content_stats(&mut stats, &mutation, None);

            assert_eq!(
                stats.tombstone_histogram.size(),
                1,
                "a DeleteRow on the carries_static path must fold its LDT into \
                 the tombstone-drop-time histogram exactly once, not twice"
            );
        }

        /// The extracted single-mutation-group helper both `KWayMerger::merge`
        /// and `WriteEngine::maintenance_step` call (issue #4246 roborev
        /// round-3 finding #4) — exercised directly, the SAME function both
        /// production paths use, rather than only reachable through whichever
        /// integration test happens to exercise one of them.
        #[test]
        fn fold_single_mutation_row_group_folds_deletion_marker_exactly_once() {
            let mutation = delete_row(1, 42);
            let mut stats = StatisticsMetadata::new();
            fold_single_mutation_row_group(&mut stats, &mutation, &schema(), false, None);
            assert_eq!(stats.min_timestamp, 42);
            assert_eq!(
                stats.tombstone_histogram.size(),
                1,
                "the row's own deletion marker must be folded exactly once \
                 through the shared single-mutation-group helper"
            );
        }

        /// A live, surviving mutation folds its content normally.
        #[test]
        fn fold_single_mutation_row_group_folds_surviving_content() {
            let mutation = insert(1, 99);
            let mut stats = StatisticsMetadata::new();
            fold_single_mutation_row_group(&mut stats, &mutation, &schema(), false, None);
            assert_eq!(stats.min_timestamp, 99);
        }

        /// A live mutation fully covered by `shadow_floor` folds nothing —
        /// proving the helper threads `shadow_floor` through to
        /// `row_group_survival`, not just a hardcoded `None`.
        #[test]
        fn fold_single_mutation_row_group_gates_shadowed_content() {
            let mutation = insert(1, 5);
            let mut stats = StatisticsMetadata::new();
            fold_single_mutation_row_group(&mut stats, &mutation, &schema(), false, Some(10));
            assert_eq!(
                stats.min_timestamp,
                i64::MAX,
                "a live mutation entirely covered by shadow_floor=10 must fold nothing"
            );
        }
    }
}
