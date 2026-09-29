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
///
/// PERFORMANCE (issue #4288, roborev round-5 finding): this calls
/// `merge_row_group` as a pre-check, and every call site ALSO calls it again
/// (directly or via `feed_row`/`merge_clustering_rows`) to actually emit the
/// row — `merge_row_group`'s own full per-column LWW reconciliation
/// (allocating `cells`/`whole_col_liveness`/`complex_element_ops`/`ops`) now
/// runs 2-3x per row group on the write hot path instead of once.
/// `merge_row_group` is a pure function of its arguments, so a future
/// refactor computing the group's `RowWrite` once and deriving both the
/// stats-fold decision and the emission from it would remove this
/// duplication; deferred out of #4246's own scope (a correctness fix) to
/// avoid a broader refactor under time pressure.
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

/// The fold sequence for a STATIC-ROW CARRIER mutation — the
/// `clustering_key: None` shape both incremental compaction paths classify
/// out of `row_mutations` and handle on their own branch
/// (`KWayMerger::merge` and `WriteEngine::maintenance_step`).
///
/// Extracted (issue #4246 roborev round-6 finding): those two branches used
/// to call [`fold_row_content_stats`] ALONE, which meant a static carrier's
/// own row deletion was folded NOWHERE on either compaction path.
/// [`fold_row_content_stats`] deliberately does not fold a row's deletion —
/// neither the `DeleteRow` op (its match arm is an explicit no-op) nor the
/// #932 decoupled `row_tombstone` field — because a row's deletion is folded
/// exactly once at the GROUP level via [`fold_row_deletion_marker`]. The
/// static-carrier branches never performed that group-level fold, so both
/// representations were silently dropped from persisted stats. Before the
/// #4246 refactor `fold_mutation_stats` folded them for these mutations
/// (issue #1721's explicit purpose: without the LDT contribution
/// `min_local_deletion_time` stays `i32::MAX` and `data_writer/rows.rs`'s
/// below-baseline guard then REJECTS the row). The flush path is shielded by
/// `compute_mutations_baseline_stats`/`pre_seed_encoding_baselines`; neither
/// compaction path has such a pre-seed.
///
/// Resolution goes through `DataWriter::resolve_row_deletion` — the single
/// authority, which picks whichever of the two representations carries the
/// GREATER timestamp — rather than a local hand-rolled preference, so this
/// can never drift from what `merge_row_group` actually emits.
///
/// `shadow_floor` is `None`: static-cell shadowing uses a separate,
/// partition-floor-only mechanism outside this fix's verified scope (see
/// [`row_group_survives`]'s doc comment), so the carrier's content keeps its
/// prior unconditional-fold behavior exactly.
pub(crate) fn fold_static_carrier_stats(stats: &mut StatisticsMetadata, mutation: &Mutation) {
    fold_row_content_stats(stats, mutation, None);
    fold_row_deletion_marker(stats, DataWriter::resolve_row_deletion(&[mutation], None));
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
/// timestamp, row-level TTL, and `WriteWithTtl`/`Delete`/`DeleteRow`/`Write`
/// contributions) is shadow-gated on `mutation.timestamp_micros <= dts` —
/// matching `merge_row_group`'s own `mutation_shadowed` test EXACTLY, with
/// NO exemption for a mutation that itself carries a `DeleteRow` op or a
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
/// A SECOND, per-cell filter mirrors `merge_row_group`'s own
/// (`data_writer/rows.rs`, issue #1018 roborev HIGH / issue #4246 roborev
/// round-4 Medium finding): even when the mutation's ROW timestamp survives
/// (`!mutation_shadowed`), an individual `Write`/`WriteWithTtl`/`Delete` cell
/// may carry its OWN (lower) per-cell timestamp in
/// `Mutation::cell_write_timestamps` (populated by the compaction
/// merge→mutation path) that is itself `<= dts` — covered by the
/// partition/range/row tombstone and never emitted, even though the row
/// survives via other content. Each of those three arms resolves its own
/// `cell_ts` via `Mutation::cell_write_timestamp` (falling back to
/// `mutation.timestamp_micros` when no per-cell override exists, a no-op
/// for the common single-writetime row) and gates on it independently of
/// `mutation_shadowed`.
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
                column,
                ttl_seconds,
                local_deletion_time,
                ..
            } => {
                if mutation_shadowed {
                    continue;
                }
                // Issue #1018 (roborev round-4 Medium finding, mirroring
                // `merge_row_group`'s identical per-cell filter,
                // `data_writer/rows.rs`): a mutation whose ROW timestamp
                // survives can still carry an individual cell whose OWN
                // resolved timestamp (`Mutation::cell_write_timestamps`,
                // populated by the compaction merge→mutation path) is
                // `<= shadow_boundary` — covered by the partition/range/row
                // tombstone and never emitted, even though the row itself
                // survives via other content. `cell_write_timestamp` falls
                // back to `mutation.timestamp_micros` when no per-cell
                // override exists, so this is a no-op for the common
                // single-writetime row.
                let cell_ts = mutation.cell_write_timestamp(column);
                if shadow_boundary.is_some_and(|dts| cell_ts <= dts) {
                    continue;
                }
                stats.update_timestamp(cell_ts);
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
            op @ CellOperation::Delete { column, .. } => {
                if mutation_shadowed {
                    continue;
                }
                // Per-cell shadow filter (roborev round-4 Medium finding) —
                // see the identical comment on `WriteWithTtl` above.
                let cell_ts = mutation.cell_write_timestamp(column);
                if shadow_boundary.is_some_and(|dts| cell_ts <= dts) {
                    continue;
                }
                stats.update_timestamp(cell_ts);
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
            CellOperation::Write { column, value, .. } => {
                if mutation_shadowed {
                    continue;
                }
                // Per-cell shadow filter (roborev round-4 Medium finding) —
                // see the identical comment on `WriteWithTtl` above.
                let cell_ts = mutation.cell_write_timestamp(column);
                if shadow_boundary.is_some_and(|dts| cell_ts <= dts) {
                    continue;
                }
                stats.update_timestamp(cell_ts);
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
#[path = "stats_fold_tests.rs"]
mod tests;
