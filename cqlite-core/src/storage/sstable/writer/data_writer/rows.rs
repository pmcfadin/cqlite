//! Clustering-row emission: row merge/reconciliation, merged row bodies, clustering prefixes, column bitmaps, and column ordering helpers.
//!
//! Part of the `data_writer` responsibility split (issue #1118): this module
//! holds one `impl DataWriter` block. `use super::*` pulls the shared writer
//! types, serialization/schema helpers, flag constants, and crate imports
//! re-exported from `data_writer/mod.rs`. No emitted bytes change.

use super::*;

use crate::storage::write_engine::reconcile_rules;
// `ReconcileCell` is imported for its methods (`is_tombstone`) used by the
// shared `Cells#reconcile` tie-break in `merge_row_group` (issue #947).
use crate::storage::write_engine::reconcile_rules::ReconcileCell;

impl DataWriter {
    /// Write a single row
    ///
    /// This implements the V5CompressedLegacy row format with delta encoding.
    #[allow(dead_code)]
    pub(super) fn write_row(&mut self, mutation: &Mutation, schema: &TableSchema) -> Result<()> {
        self.write_row_with_prev_size(mutation, schema, 0)?;
        Ok(())
    }

    /// Write a single mutation as one row. Thin adapter over the merged-row
    /// path so legacy callers (and unit tests) keep working.
    pub(super) fn write_row_with_prev_size(
        &mut self,
        mutation: &Mutation,
        schema: &TableSchema,
        prev_size: u64,
    ) -> Result<usize> {
        match Self::merge_row_group(&[mutation], schema, false, None) {
            Some(row) => {
                let (bytes, _cells) =
                    self.write_merged_row_with_prev_size(&row, schema, prev_size)?;
                Ok(bytes)
            }
            // Nothing to write (e.g. a tombstone-carrier mutation with no ops)
            None => Ok(0),
        }
    }

    /// Group same-clustering mutations of a partition and merge each group
    /// into a single [`RowWrite`].
    ///
    /// Mutations must already be sorted by clustering key (the caller —
    /// `SSTableWriter::write_partition` — sorts them); grouping is by
    /// adjacency. Pure-static mutations are excluded (their cells live in the
    /// static-row prelude), and groups that merge to nothing (e.g. mutations
    /// that exist only to carry partition/range tombstones) produce no row.
    pub(super) fn merge_clustering_rows<'a>(
        &self,
        mutations: &'a [Mutation],
        schema: &TableSchema,
        skip_static_ops: bool,
        partition_floor: Option<i64>,
        range_tombstones: &[RangeTombstone],
    ) -> Vec<RowWrite<'a>> {
        let row_mutations: Vec<&'a Mutation> = mutations
            .iter()
            .filter(|m| !is_static_row_mutation(m, schema))
            .collect();

        let mut rows = Vec::new();
        let mut start = 0;
        while start < row_mutations.len() {
            let mut end = start + 1;
            while end < row_mutations.len()
                && row_mutations[end].clustering_key == row_mutations[start].clustering_key
            {
                end += 1;
            }

            // Shadow floor for this row: partition tombstone plus any range
            // tombstone covering the group's clustering key.
            let clustering_key = row_mutations[start].clustering_key.as_ref();
            let mut shadow_floor = partition_floor;
            for rt in range_tombstones {
                if range_tombstone_covers(rt, clustering_key, schema) {
                    shadow_floor =
                        Some(shadow_floor.map_or(rt.deletion_time, |f| f.max(rt.deletion_time)));
                }
            }

            if let Some(row) = Self::merge_row_group(
                &row_mutations[start..end],
                schema,
                skip_static_ops,
                shadow_floor,
            ) {
                rows.push(row);
            }
            start = end;
        }
        rows
    }

    /// Merge a group of mutations sharing one clustering key into a single
    /// row, applying Cassandra reconciliation semantics at write time:
    ///
    /// - Row deletion: the newest `DeleteRow` wins; mutations at or before
    ///   the deletion timestamp are shadowed (`DeletionTime.deletes` uses
    ///   `timestamp <= markedForDeleteAt`).
    /// - Cells: last-write-wins per column by timestamp; a tombstone wins a
    ///   timestamp tie (Cassandra cell reconciliation).
    /// - Liveness: from the newest surviving mutation that writes cells, or
    ///   a pure primary-key insert (no ops and no tombstone payload). Pure
    ///   row tombstones carry NO liveness, matching Cassandra's serializer.
    ///
    /// Returns `None` when the group produces no row at all (e.g. a mutation
    /// that exists only to carry a partition or range tombstone, or a row
    /// fully shadowed by the partition/range tombstone `shadow_floor`).
    pub(super) fn merge_row_group<'a>(
        group: &[&'a Mutation],
        schema: &TableSchema,
        skip_static_ops: bool,
        shadow_floor: Option<i64>,
    ) -> Option<RowWrite<'a>> {
        use crate::storage::write_engine::mutation::CellOperation;

        // Newest row deletion in the group (if any). A row deletion at or
        // before the shadow floor is redundant (the partition/range tombstone
        // already covers it) and is dropped.
        let mut row_deletion: Option<(i64, i32)> = None;
        for m in group {
            let has_delete_row = m
                .operations
                .iter()
                .any(|op| matches!(op, CellOperation::DeleteRow));
            if has_delete_row
                && shadow_floor.is_none_or(|floor| m.timestamp_micros > floor)
                && row_deletion.is_none_or(|(ts, _)| m.timestamp_micros >= ts)
            {
                // Issue #764: honor the mutation's explicit local_deletion_time.
                row_deletion = Some((m.timestamp_micros, m.effective_local_deletion_time()));
            }
            // Issue #932: an explicit coexisting row tombstone carries a deletion
            // time DECOUPLED from `m.timestamp_micros` (the row's liveness
            // writetime). Select it by its OWN `deletion_time`, not the mutation
            // timestamp. The mutation's surviving cells were written strictly
            // after this deletion, so they are NOT shadowed by it (the shadow
            // boundary below uses `deletion_ts`, which equals the deletion's own
            // time) — the row keeps both the deletion AND the newer cells.
            if let Some((del_ts, del_ldt)) = m.row_tombstone {
                if shadow_floor.is_none_or(|floor| del_ts > floor)
                    && row_deletion.is_none_or(|(ts, _)| del_ts >= ts)
                {
                    row_deletion = Some((del_ts, del_ldt));
                }
            }
        }
        // Cells and liveness are shadowed by the strongest covering deletion:
        // the row deletion or the partition/range tombstone floor.
        let deletion_ts = match (row_deletion.map(|(ts, _)| ts), shadow_floor) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };

        // Per-column last-write-wins; tombstones win timestamp ties.
        let mut cells: std::collections::HashMap<&'a str, MergedOp<'a>> =
            std::collections::HashMap::new();
        // Epic #899 (Phase B): per-element complex ops are NOT deduped per column
        // (a column has many elements). Kept verbatim and emitted via
        // `write_complex_column_per_element`. Empty for every existing scenario.
        let mut complex_element_ops: Vec<MergedOp<'a>> = Vec::new();
        // Liveness: (timestamp, row-level TTL) of the newest contributing mutation.
        // Cell-less liveness sources (pure-PK inserts, clustering-only writes) are
        // recorded here directly (they are never shadowed by complex-deletion markers).
        let mut liveness: Option<(i64, Option<u32>)> = None;
        // Issue #921 (roborev High): WHOLE-COLUMN write liveness candidates, keyed by
        // regular-column name → (newest write ts, that write's row TTL). A
        // `Write`/`WriteWithTtl` of a regular column sets the ROW MARKER (row
        // liveness) even when its CELL loses last-write-wins to a same-column
        // `Delete` tombstone at an equal timestamp (issue #822) — the row marker is
        // independent of cell-level reconciliation. So these candidates are collected
        // here REGARDLESS of the per-column `cells` LWW outcome. They are folded into
        // `liveness` only AFTER the #927 mixed-stream reconcile, with candidates for
        // any column whose COMPLEX stream wins removed first: a whole-column write
        // entirely superseded by a newer complex marker/element stream leaves no live
        // cell and must NOT leak a phantom row marker.
        let mut whole_col_liveness: std::collections::HashMap<&'a str, (i64, Option<u32>)> =
            std::collections::HashMap::new();
        // Issue #887 / #921: a live `WriteComplexElement` contributes row liveness too,
        // but ONLY if it actually survives every reconcile/retain below. Rather than
        // tracking pre-retain candidates with an exclusion set (fragile — roborev
        // #921 High), complex-element liveness is DERIVED at the very end directly
        // from the FINAL surviving `complex_element_ops`: after the #927 mixed-stream
        // reconcile AND the #887 strict-supersede/shadow-before-purge retain have run,
        // each surviving LIVE element (a `WriteComplexElement` with a value, not an
        // element tombstone) folds its OWN `(elem_ts, ttl)` into `liveness`. This way
        // liveness reflects exactly the live per-element cells that remain in the
        // output — no candidate can resurrect liveness for an element that was dropped
        // (by a winning whole-column op, LIVE or Delete) or shadowed (by a same-column
        // complex deletion). Simple-cell and pure-PK liveness in `liveness` is folded
        // directly below and is never shadowed by complex-deletion markers.

        for m in group {
            // Shadowed by the row deletion (or partition/range floor): cells,
            // liveness, and row-level writes of this mutation written at
            // `timestamp <= deletion_ts` are dead.
            //
            // Issue #887: a `ComplexDeletion` marker is itself a deletion with its OWN
            // `marked_for_delete_at`, NOT the mutation's row timestamp. A marker whose
            // mfda STRICTLY exceeds `deletion_ts` covers a range the row/partition
            // tombstone does not (e.g. elements in OTHER SSTables not part of this
            // compaction), so it must survive even when the carrying mutation's row
            // timestamp is shadowed. The SAME independence applies to
            // `WriteComplexElement`: each per-element complex write carries its OWN
            // `timestamp_micros` (an explicit delta, NOT the mutation's row timestamp);
            // an element whose own timestamp STRICTLY exceeds `deletion_ts` is live and
            // must survive.
            //
            // Skip the rest of a shadowed mutation, but still scan it for such
            // surviving complex-deletion markers AND surviving per-element writes so
            // they are emitted alongside the row tombstone.
            let mutation_shadowed = deletion_ts.is_some_and(|dts| m.timestamp_micros <= dts);
            if mutation_shadowed {
                // The mutation's row timestamp is covered, so its simple cells,
                // liveness, and row-level writes are dead. Per-element/per-marker
                // complex ops carry INDEPENDENT timestamps, however, so still scan for
                // them and push verbatim — carrying each op's OWN timestamp in the
                // `MergedOp` (see the per-op rationale on the normal path below). The
                // `deletion_ts` shadow boundary is applied UNIFORMLY for both paths by
                // the single retain pass after this loop (Issue #921 roborev), so no
                // per-op boundary check is done here; that keeps the normal and
                // shadowed paths from drifting apart.
                for op in &m.operations {
                    match op {
                        CellOperation::ComplexDeletion {
                            marked_for_delete_at,
                            ..
                        } => {
                            if skip_static_ops && is_static_operation(op, schema) {
                                continue;
                            }
                            complex_element_ops.push(MergedOp {
                                op,
                                timestamp_micros: *marked_for_delete_at,
                                row_ttl_seconds: m.ttl_seconds,
                                cell_local_deletion_time: m.effective_local_deletion_time(),
                            });
                        }
                        CellOperation::WriteComplexElement {
                            timestamp_micros: elem_ts,
                            ..
                        } => {
                            if skip_static_ops && is_static_operation(op, schema) {
                                continue;
                            }
                            complex_element_ops.push(MergedOp {
                                op,
                                timestamp_micros: *elem_ts,
                                row_ttl_seconds: m.ttl_seconds,
                                cell_local_deletion_time: m.effective_local_deletion_time(),
                            });
                        }
                        _ => {}
                    }
                }
                continue;
            }

            // Issue #921: a `Write`/`WriteWithTtl` of a PRIMARY-KEY column (the
            // compaction path can surface a clustering column as a Write, #857) is
            // dropped from `cells` below — it leaves NO survivor to derive liveness
            // from. Such a clustering-only write must still keep the row live, so
            // record its contribution inline (it can never be shadowed by a complex
            // marker — primary-key columns are never complex).
            let mut pk_write_liveness = false;
            for op in &m.operations {
                let column = match op {
                    CellOperation::Write { column, .. }
                    | CellOperation::WriteWithTtl { column, .. }
                    | CellOperation::Delete { column, .. } => column.as_str(),
                    // Epic #899 (Phase B): per-element complex ops keep all
                    // elements (no per-column dedup). A live element write
                    // contributes row liveness; a `ComplexDeletion` marker does
                    // not. Primary-key columns can never be complex, so no
                    // key-column skip is needed.
                    //
                    // Issue #887: the liveness contribution is DEFERRED. If this
                    // element is later shadowed by a same-column `ComplexDeletion`
                    // (shadow-before-purge retain below), it must NOT keep the row
                    // live. Record a candidate carrying the column + the element's OWN
                    // timestamp; fold it into `liveness` only if it survives the retain.
                    CellOperation::WriteComplexElement {
                        timestamp_micros: elem_ts,
                        ..
                    } => {
                        if skip_static_ops && is_static_operation(op, schema) {
                            continue;
                        }
                        // Issue #887 / #921: row liveness from a live per-element write
                        // is NOT recorded here. It is derived from the FINAL surviving
                        // `complex_element_ops` after all reconciles/retains (see the
                        // end of this function), so an element later dropped or
                        // shadowed cannot leak liveness.
                        //
                        // Issue #921 (roborev Finding 1): carry the element's OWN
                        // `*elem_ts` in the `MergedOp`, NOT the mutation row timestamp.
                        // #927's mixed-stream reconcile below derives `elem_max_ts`
                        // from `MergedOp.timestamp_micros`; using the mutation row
                        // timestamp would understate it and let an older whole-column
                        // write wrongly shadow a newer per-element edit whose enclosing
                        // mutation happens to carry an older row timestamp. This also
                        // matches the shadowed/rescue path above (which already stores
                        // `*elem_ts`) and the #887 shadow-before-purge retain. The
                        // per-element writer reads the element's own timestamp from the
                        // `WriteComplexElement` payload, so the emitted cell timestamp
                        // is unaffected.
                        complex_element_ops.push(MergedOp {
                            op,
                            timestamp_micros: *elem_ts,
                            row_ttl_seconds: m.ttl_seconds,
                            cell_local_deletion_time: m.effective_local_deletion_time(),
                        });
                        continue;
                    }
                    CellOperation::ComplexDeletion {
                        marked_for_delete_at,
                        ..
                    } => {
                        if skip_static_ops && is_static_operation(op, schema) {
                            continue;
                        }
                        // Issue #921 (roborev Finding): a `ComplexDeletion` marker is a
                        // deletion with its OWN `marked_for_delete_at` (mfda), which is
                        // INDEPENDENT of the enclosing mutation's row timestamp (the
                        // marker can be carried by an OLDER metadata/tombstone mutation).
                        // #927's mixed-stream reconcile below derives `elem_max_ts` from
                        // `MergedOp.timestamp_micros` and uses `mop.timestamp_micros >=
                        // emax` to decide whether a whole-column op shadows the
                        // per-element/marker stream. Carrying the row timestamp here
                        // would let a whole-column write/delete at `row_ts > mfda` but
                        // `whole_ts < mfda` wrongly drop a NEWER collection tombstone —
                        // losing the marker and resurrecting covered elements. Carry the
                        // mfda so the comparison reflects the marker's true deletion time.
                        // The emitted marker bytes read mfda/ldt from the
                        // `ComplexDeletion` payload (see
                        // `write_complex_element_columns`), so this only affects the
                        // RECONCILE COMPARISON, never the serialized marker.
                        complex_element_ops.push(MergedOp {
                            op,
                            timestamp_micros: *marked_for_delete_at,
                            row_ttl_seconds: m.ttl_seconds,
                            cell_local_deletion_time: m.effective_local_deletion_time(),
                        });
                        continue;
                    }
                    CellOperation::DeleteRow => continue,
                };
                if skip_static_ops && is_static_operation(op, schema) {
                    continue;
                }
                // Issue #921 (roborev High): whole-column `Write`/`WriteWithTtl`
                // liveness is NOT folded into `liveness` here anymore. It is recorded
                // as a per-column candidate in `whole_col_liveness` (below the
                // cells-dedup block) and folded only AFTER the #927 mixed-stream
                // reconcile, with candidates for columns whose complex stream wins
                // removed. Folding inline before reconcile let a whole-column write
                // entirely superseded by a newer complex marker/element stream leak a
                // phantom row marker.
                // Primary-key columns are encoded positionally (partition key +
                // clustering prefix), never as cells. The compaction path can
                // surface a clustering column as a Write op (#857) — drop it so the
                // writer doesn't emit a phantom cell that corrupts the row body for
                // strict readers.
                if is_primary_key_column(column, schema) {
                    if matches!(
                        op,
                        CellOperation::Write { .. } | CellOperation::WriteWithTtl { .. }
                    ) {
                        // A clustering-only write keeps the row live even though it
                        // produces no cell survivor (see note above).
                        pk_write_liveness = true;
                    }
                    continue;
                }

                // Issue #921: record the ROW-MARKER liveness candidate for this
                // regular-column write. Done BEFORE the cells LWW dedup so that a
                // same-column `Delete` cell tombstone winning an equal-ts tie does not
                // erase the row marker (issue #822). Keep the newest write's ts/ttl.
                if matches!(
                    op,
                    CellOperation::Write { .. } | CellOperation::WriteWithTtl { .. }
                ) {
                    match whole_col_liveness.entry(column) {
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert((m.timestamp_micros, m.ttl_seconds));
                        }
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            if m.timestamp_micros >= entry.get().0 {
                                entry.insert((m.timestamp_micros, m.ttl_seconds));
                            }
                        }
                    }
                }

                let candidate = MergedOp {
                    op,
                    timestamp_micros: m.timestamp_micros,
                    row_ttl_seconds: m.ttl_seconds,
                    // #921 finding 2: a surviving `Delete` cell tombstone keeps its
                    // OWN surfaced LDT; other ops fall back to the mutation's LDT.
                    cell_local_deletion_time: op_cell_local_deletion_time(op, m),
                };
                match cells.entry(column) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(candidate);
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        let existing = entry.get();
                        // Per-cell winner resolution. The SHARED Cassandra
                        // `Cells#reconcile` tie-break (issue #947,
                        // `reconcile_rules::cell_wins`) decides the load-bearing
                        // axes — higher timestamp wins, and at EQUAL timestamp a
                        // cell DELETION (`Delete` tombstone) beats a LIVE/EXPIRING
                        // write BEFORE any localDeletionTime compare (#848/#498;
                        // `WriteWithTtl` is LIVE, not a tombstone).
                        //
                        // The writer overlays a WRITER-ONLY, order-dependent
                        // last-write-wins tie-break for the one case the shared
                        // rule leaves to the caller: at EQUAL timestamp with EQUAL
                        // liveness the later-applied mutation wins (keep-last).
                        // Cassandra's reconcile is order-independent and the merge
                        // path keeps first-seen there; this overlay reproduces the
                        // writer's historical convention exactly and is NOT part of
                        // the shared rule.
                        let wins = reconcile_rules::cell_wins(&candidate, existing)
                            || (candidate.timestamp_micros == existing.timestamp_micros
                                && candidate.is_tombstone() == existing.is_tombstone());
                        if wins {
                            entry.insert(candidate);
                        }
                    }
                }
            }

            // A mutation with no ops and no tombstone payload is a pure
            // primary-key insert: it creates row liveness but no cells.
            let pure_pk_insert = m.operations.is_empty()
                && m.partition_tombstone.is_none()
                && m.range_tombstones.is_empty();
            // Issue #921: only the cell-less liveness sources are folded inline here
            // — a pure-PK insert and a clustering-only write. Whole-column writes
            // that DO produce a `cells` survivor are derived after reconcile.
            if (pk_write_liveness || pure_pk_insert)
                && liveness.is_none_or(|(ts, _)| m.timestamp_micros >= ts)
            {
                liveness = Some((m.timestamp_micros, m.ttl_seconds));
            }
        }

        // Issue #921 (roborev HIGH/MEDIUM): apply the row/range `deletion_ts` shadow
        // boundary UNIFORMLY to per-element/per-marker complex ops, regardless of
        // whether the enclosing mutation was classified shadowed. Each such op carries
        // its OWN timestamp (a `WriteComplexElement`'s explicit delta, a
        // `ComplexDeletion`'s `marked_for_delete_at`), already stored in
        // `MergedOp.timestamp_micros` at every push point above. Previously this
        // boundary was applied ONLY on the shadowed/rescue path; the normal path
        // (mutation row ts > deletion_ts) pushed complex ops unconditionally, so a
        // covered element (`elem_ts <= deletion_ts`) resurrected after the tombstone
        // was purged, and a fully-covered marker (`mfda <= deletion_ts`) emitted a dead
        // redundant tombstone. One retain here gives both paths the SAME boundary:
        // `> deletion_ts` survives, `<= deletion_ts` is shadowed (equal-ts tombstone
        // wins, #498). This is the ONLY complex-op family with an independent timestamp;
        // simple `Write`/`Delete` and pure-PK liveness keep their mutation-level shadow
        // behavior (handled by `mutation_shadowed` above).
        if let Some(dts) = deletion_ts {
            complex_element_ops.retain(|mop| mop.timestamp_micros > dts);
        }

        let mut ops: Vec<MergedOp<'a>> = cells.into_values().collect();

        // Issue #927 (item 6): mixed-stream reconciliation. A single column may
        // carry BOTH a whole-column op (in `ops`) and per-element complex ops (in
        // `complex_element_ops`) — e.g. a UDT overwritten wholesale by one mutation
        // and edited per-field by another. Emitting both would double-write the
        // column and desync the reader. Reconcile by timestamp shadowing: keep the
        // stream with the newer max timestamp and drop the other, rather than
        // silently losing one.
        //
        // Issue #921 (roborev High): liveness is no longer tracked via a candidate
        // list + exclusion set here. Because complex-element liveness is DERIVED at
        // the end from the FINAL surviving `complex_element_ops`, a per-element
        // stream dropped by a winning whole-column op (LIVE write OR Delete) simply
        // leaves no survivor to fold — so it cannot leak liveness, and this
        // reconcile only has to drop the losing stream.
        if !complex_element_ops.is_empty() {
            let mut elem_max_ts: std::collections::HashMap<&str, i64> =
                std::collections::HashMap::new();
            for mop in &complex_element_ops {
                if let Some(col) = merged_op_column(mop.op) {
                    let entry = elem_max_ts.entry(col).or_insert(i64::MIN);
                    if mop.timestamp_micros > *entry {
                        *entry = mop.timestamp_micros;
                    }
                }
            }
            // Columns whose whole-column op wins (>= element max ts) keep their
            // whole op; their per-element ops are dropped. Columns where elements
            // win drop the whole-column op.
            let mut whole_wins: std::collections::HashSet<&str> = std::collections::HashSet::new();
            ops.retain(|mop| match merged_op_column(mop.op) {
                Some(col) => match elem_max_ts.get(col) {
                    Some(&emax) if mop.timestamp_micros >= emax => {
                        whole_wins.insert(col);
                        true
                    }
                    Some(_) => false,
                    None => true,
                },
                None => true,
            });
            if !whole_wins.is_empty() {
                complex_element_ops.retain(|mop| match merged_op_column(mop.op) {
                    Some(col) => !whole_wins.contains(col),
                    None => true,
                });
            }
            // Issue #921 (roborev High): a column whose COMPLEX stream won (present in
            // `elem_max_ts` but NOT in `whole_wins`) had its whole-column op dropped
            // from `ops`. That whole-column write is entirely superseded — it leaves
            // no live cell — so it must NOT contribute the row marker. Drop its
            // liveness candidate so a phantom live row is not emitted.
            if !whole_col_liveness.is_empty() {
                whole_col_liveness
                    .retain(|col, _| !elem_max_ts.contains_key(col) || whole_wins.contains(col));
            }
        }

        // Issue #887: SHADOW-BEFORE-PURGE for the direct writer merge path — the
        // writer-side analogue of reconcile_cluster Step 2b (merge.rs). Above, a
        // `WriteComplexElement` is kept based only on its timestamp vs the ROW/RANGE
        // `deletion_ts`; it is NOT yet shadowed against a surviving `ComplexDeletion`
        // marker for the SAME column. A mutation set carrying e.g.
        // `ComplexDeletion(tags, mfda=300)` and an element `tags[path]@200` would
        // otherwise emit BOTH the marker and the covered element@200 — violating
        // shadow-before-purge (a later purge of the marker resurrects the element).
        //
        // Mirror Step 2b exactly: (1) reduce to the ACTIVE complex deletion PER COLUMN
        // NAME (strict-supersede: greatest `marked_for_delete_at` wins; EQUAL does NOT
        // supersede), then (2) drop every `WriteComplexElement` of that column whose
        // own `timestamp_micros <= marked_for_delete_at` (shadowed). Boundary: element
        // ts STRICTLY GREATER than mfda survives; `<=` is shadowed. Matched BY COLUMN
        // NAME. This runs unconditionally so the normal mutation path and the
        // shadowed-mutation rescue path are consistent.
        if !complex_element_ops.is_empty() {
            // Strict-supersede: active mfda per column name from surviving markers.
            let mut active_mfda: std::collections::HashMap<&'a str, i64> =
                std::collections::HashMap::new();
            for mop in &complex_element_ops {
                if let CellOperation::ComplexDeletion {
                    column,
                    marked_for_delete_at,
                    ..
                } = mop.op
                {
                    let col = column.as_str();
                    match active_mfda.get(col) {
                        // STRICTLY GREATER supersedes; equal/lesser does NOT.
                        // Shared strict-supersede rule (issue #947).
                        Some(existing)
                            if !reconcile_rules::complex_deletion_supersedes(
                                *marked_for_delete_at,
                                *existing,
                            ) => {}
                        _ => {
                            active_mfda.insert(col, *marked_for_delete_at);
                        }
                    }
                }
            }
            // Shadow-before-purge: drop any WriteComplexElement whose own timestamp is
            // `<= mfda` of the active marker on its column.
            if !active_mfda.is_empty() {
                complex_element_ops.retain(|mop| match mop.op {
                    CellOperation::WriteComplexElement {
                        column,
                        timestamp_micros: elem_ts,
                        ..
                    } => active_mfda.get(column.as_str()).is_none_or(|mfda| {
                        // Shared shadow-before-purge boundary (issue #947).
                        reconcile_rules::element_survives_complex_deletion(*elem_ts, *mfda)
                    }),
                    _ => true,
                });
            }
        }

        // Issue #921 (roborev High): FOLD the surviving WHOLE-COLUMN write liveness
        // candidates — i.e. AFTER the #927 mixed-stream reconcile, which removed
        // candidates for any column whose complex stream won (the whole-column write
        // was entirely superseded by a newer complex marker/element stream). The
        // remaining candidates are regular-column `Write`/`WriteWithTtl` ops that set
        // the ROW MARKER: each contributes even when its CELL lost a same-column
        // equal-ts `Delete` tie (issue #822) — the row marker is independent of
        // cell-level reconciliation. This is the symmetric counterpart to the
        // per-element liveness derivation below: a whole-column write dropped by a
        // winning complex stream (e.g. `Write(tags)@500` + `ComplexDeletion(tags,
        // mfda=900)`) is no longer in `whole_col_liveness`, so it cannot leak a
        // phantom live row. Cell-less liveness sources (pure-PK insert,
        // clustering-only write) were already folded inline above.
        for &(ts, ttl) in whole_col_liveness.values() {
            if liveness.is_none_or(|(cur, _)| ts >= cur) {
                liveness = Some((ts, ttl));
            }
        }

        // Issue #921 (roborev High): DERIVE complex-element liveness from the FINAL
        // surviving `complex_element_ops` — i.e. AFTER the #927 mixed-stream
        // reconcile AND the #887 strict-supersede / shadow-before-purge retain. Each
        // surviving LIVE element (a `WriteComplexElement` carrying a value, not an
        // element tombstone and not a `ComplexDeletion` marker) folds its OWN
        // `(elem_ts, ttl)` into row liveness. Because we read only the survivors, an
        // element dropped by a winning whole-column op (LIVE write OR Delete) or
        // shadowed by a same-column complex deletion contributes nothing — it is no
        // longer in `complex_element_ops`. A row whose only live complex elements
        // were all dropped/shadowed therefore carries NO complex-element liveness.
        // Simple-cell `Write`/`WriteWithTtl` and pure-PK liveness already folded into
        // `liveness` above are independent and untouched here.
        //
        // The element's OWN `timestamp_micros` and `ttl_seconds` are read straight
        // from its `WriteComplexElement` payload (the same values the writer stamps
        // on the surviving element cell), so row liveness reflects exactly that live
        // element.
        for mop in &complex_element_ops {
            if let CellOperation::WriteComplexElement {
                value,
                timestamp_micros: elem_ts,
                ttl_seconds,
                is_deleted,
                ..
            } = mop.op
            {
                // A live element has a value and is not a tombstone. Element-level
                // tombstones (`value == None` / `is_deleted`) and empty-value
                // members carry no liveness.
                if *is_deleted || value.is_none() {
                    continue;
                }
                if liveness.is_none_or(|(ts, _)| *elem_ts >= ts) {
                    liveness = Some((*elem_ts, *ttl_seconds));
                }
            }
        }

        if ops.is_empty()
            && complex_element_ops.is_empty()
            && row_deletion.is_none()
            && liveness.is_none()
        {
            return None;
        }

        Some(RowWrite {
            clustering_key: group[0].clustering_key.as_ref(),
            liveness_ts: liveness.map(|(ts, _)| ts),
            ttl_seconds: liveness.and_then(|(_, ttl)| ttl),
            row_deletion,
            ops,
            complex_element_ops,
        })
    }

    /// Write one merged row (flags + clustering prefix + sizes + body).
    /// Write a merged row and return `(bytes_written, cells_written)`.
    ///
    /// Issue #851 (review): `cells_written` is the count of cells physically
    /// serialized for this row (from `build_merged_row_body` →
    /// `write_merged_cells`), so the caller's emit tally equals Data.db. It is 0
    /// for pure row tombstones and for rows whose only writes are null-valued.
    pub(super) fn write_merged_row_with_prev_size(
        &mut self,
        row: &RowWrite<'_>,
        schema: &TableSchema,
        prev_size: u64,
    ) -> Result<(usize, u64)> {
        use crate::storage::write_engine::mutation::CellOperation;

        let start_len = self.buffer.len();

        // Build row header flags
        let mut flags = 0u8;

        if row.row_deletion.is_some() {
            flags |= ROW_HAS_DELETION; // 0x10
        }
        if row.liveness_ts.is_some() {
            flags |= ROW_HAS_TIMESTAMP;
            if row.ttl_seconds.is_some() {
                flags |= ROW_HAS_TTL;
            }
        }

        // All columns present if there is no deletion, all surviving ops are
        // non-NULL writes, and they cover every regular column.
        if row.row_deletion.is_none() {
            let all_writes = row.ops.iter().all(|mop| {
                matches!(
                    mop.op,
                    CellOperation::Write { .. } | CellOperation::WriteWithTtl { .. }
                )
            });
            let has_nulls = row.ops.iter().any(|mop| match mop.op {
                CellOperation::Write { value, .. } | CellOperation::WriteWithTtl { value, .. } => {
                    matches!(value, Value::Null)
                }
                _ => false,
            });
            let regular_column_count = self.regular_columns(schema).len();
            if all_writes && !has_nulls && row.ops.len() == regular_column_count {
                flags |= ROW_HAS_ALL_COLUMNS;
            }
        }

        // Check if any operation targets a complex column (non-frozen
        // collection). roborev #885 (Finding 1): a column may be present ONLY
        // via `complex_element_ops` (the per-element path), so it must be
        // considered here too — otherwise a row whose only ops are
        // WriteComplexElement/ComplexDeletion would NOT set
        // ROW_HAS_COMPLEX_DELETION and the reader would parse the column with the
        // wrong (simple-cell) layout.
        let op_targets_complex = |col_name: &str| {
            schema
                .columns
                .iter()
                .find(|c| c.name == col_name)
                .map(|c| is_complex_column(&c.data_type))
                .unwrap_or(false)
        };
        let has_complex = row.ops.iter().any(|mop| {
            let col_name = match mop.op {
                CellOperation::Write { column, .. }
                | CellOperation::WriteWithTtl { column, .. }
                | CellOperation::Delete { column, .. } => Some(column.as_str()),
                _ => None,
            };
            col_name.is_some_and(op_targets_complex)
        }) || row.complex_element_ops.iter().any(|mop| {
            let col_name = match mop.op {
                CellOperation::WriteComplexElement { column, .. }
                | CellOperation::ComplexDeletion { column, .. } => Some(column.as_str()),
                _ => None,
            };
            col_name.is_some_and(op_targets_complex)
        });
        if has_complex {
            flags |= ROW_HAS_COMPLEX_DELETION;
        }

        // Write row flags
        self.buffer.push(flags);

        // Write clustering prefix if present (before row_size)
        if let Some(clustering_key) = row.clustering_key {
            self.write_clustering_prefix(clustering_key, schema)?;
        }

        // Calculate row body size (everything after row_size VInt)
        let (row_body, cells_written) = self.build_merged_row_body(row, schema, flags)?;

        let prev_size_vint_len = unsigned_len(prev_size);

        // Write row_size (VInt) — Cassandra's serializedRowBodySize() includes
        // the prev_unfiltered_size VInt as part of the row body
        let row_body_size = prev_size_vint_len as u64 + row_body.len() as u64;
        let mut row_size_buf = Vec::new();
        encode_unsigned(row_body_size, &mut row_size_buf);
        self.buffer.extend_from_slice(&row_size_buf);

        // Write prev_unfiltered_size (VInt, inside the row body)
        encode_unsigned(prev_size, &mut self.buffer);

        // Write rest of row body
        self.buffer.extend_from_slice(&row_body);

        Ok((self.buffer.len() - start_len, cells_written))
    }

    /// Build row body (everything after row_size VInt)
    ///
    /// Returns the bytes for: timestamp, TTL, deletion, column bitmap, and cells.
    /// Build a row body from a single mutation (legacy/test entry point).
    /// Routes through the merged-row body builder.
    #[cfg(test)]
    pub(super) fn build_row_body(
        &self,
        mutation: &Mutation,
        schema: &TableSchema,
        flags: u8,
    ) -> Result<Vec<u8>> {
        let row = Self::merge_row_group(&[mutation], schema, false, None).unwrap_or(RowWrite {
            clustering_key: mutation.clustering_key.as_ref(),
            liveness_ts: Some(mutation.timestamp_micros),
            ttl_seconds: mutation.ttl_seconds,
            row_deletion: None,
            ops: Vec::new(),
            complex_element_ops: Vec::new(),
        });
        let (body, _cells) = self.build_merged_row_body(&row, schema, flags)?;
        Ok(body)
    }

    /// Build a merged row body (everything after the row_size VInt, excluding
    /// the prev_unfiltered_size VInt written by the caller).
    ///
    /// Field order per Cassandra's `UnfilteredSerializer.serializeRowBody`:
    /// liveness timestamp, TTL + expiration LDT, row deletion, columns
    /// subset, then cells. Issue #717: the columns subset is written for
    /// EVERY row lacking HAS_ALL_COLUMNS — including row tombstones.
    ///
    /// Returns the serialized body bytes and the number of cells (columns)
    /// physically written (Issue #851, review): the count is sourced from
    /// `write_merged_cells`, the only place that decides whether a cell is
    /// emitted, so Statistics' column count cannot drift from Data.db.
    pub(super) fn build_merged_row_body(
        &self,
        row: &RowWrite<'_>,
        schema: &TableSchema,
        flags: u8,
    ) -> Result<(Vec<u8>, u64)> {
        let mut body = Vec::new();

        // Write timestamp delta (if HAS_TIMESTAMP)
        //
        // Fix #644 (S6): Cassandra writes UNSIGNED VInt for all temporal deltas.
        // SerializationHeader.java:167: out.writeUnsignedVInt(timestamp - stats.minTimestamp)
        if (flags & ROW_HAS_TIMESTAMP) != 0 {
            let liveness_ts = row.liveness_ts.ok_or_else(|| {
                Error::InvalidInput(
                    "ROW_HAS_TIMESTAMP set but row has no liveness timestamp".to_string(),
                )
            })?;
            let timestamp_delta = (liveness_ts - self.stats.min_timestamp) as u64;
            encode_unsigned(timestamp_delta, &mut body);
        }

        // Write TTL delta (if HAS_TTL)
        //
        // Fix #644 (S6): Both TTL and LDT deltas are UNSIGNED VInt.
        // SerializationHeader.java:177: out.writeUnsignedVInt32(ttl - stats.minTTL)
        // SerializationHeader.java:172: out.writeUnsignedVInt32(ldt - stats.minLocalDeletionTime)
        if (flags & ROW_HAS_TTL) != 0 {
            if let Some(ttl) = row.ttl_seconds {
                let ttl_delta = ttl as i64 - self.stats.min_ttl as i64;
                if ttl_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "TTL {} is less than min_ttl {}",
                        ttl, self.stats.min_ttl
                    )));
                }
                encode_unsigned(ttl_delta as u64, &mut body);

                let local_deletion_time = self.expiring_local_deletion_time(ttl)?;
                let ldt_delta =
                    (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
                if ldt_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "Local deletion time {} is less than min_local_deletion_time {}",
                        local_deletion_time, self.stats.min_local_deletion_time
                    )));
                }
                encode_unsigned(ldt_delta as u64, &mut body);
            }
        }

        // Write deletion (if HAS_DELETION)
        if (flags & ROW_HAS_DELETION) != 0 {
            // Row tombstone: Cassandra canonical order (markedForDeleteAt first, then localDeletionTime)
            // Per SerializationHeader.writeDeletionTime(): writeTimestamp() then writeLocalDeletionTime()
            // Fix #644 (S6): both are UNSIGNED VInt.
            let (deletion_ts, local_deletion_time) = row.row_deletion.ok_or_else(|| {
                Error::InvalidInput("ROW_HAS_DELETION set but row has no deletion time".to_string())
            })?;
            let ts_delta = (deletion_ts - self.stats.min_timestamp) as u64;
            encode_unsigned(ts_delta, &mut body);

            // Issue #873: same loud guard as the single-row path — reject a
            // below-baseline row-tombstone LDT in normal time space instead of
            // silently wrapping the unsigned delta and corrupting the row body.
            // A far-future LDT (negative as i32) is legitimate and kept.
            if local_deletion_time >= 0
                && self.stats.min_local_deletion_time >= 0
                && local_deletion_time < self.stats.min_local_deletion_time
            {
                return Err(Error::InvalidInput(format!(
                    "Row tombstone: local deletion time {} is less than min_local_deletion_time {}",
                    local_deletion_time, self.stats.min_local_deletion_time
                )));
            }
            let ldt_delta =
                local_deletion_time.wrapping_sub(self.stats.min_local_deletion_time) as u32;
            encode_unsigned(ldt_delta as u64, &mut body);
        }

        // Write column bitmap (if NOT HAS_ALL_COLUMNS).
        // Issue #717: this is written even for row tombstones — Cassandra's
        // deserializer reads the subset right after the deletion times.
        if (flags & ROW_HAS_ALL_COLUMNS) == 0 {
            self.write_merged_column_bitmap(&mut body, row, schema)?;
        }

        // Write cell data (none survive for pure row tombstones)
        let cells_written = self.write_merged_cells(&mut body, row, schema)?;

        Ok((body, cells_written))
    }

    /// Write clustering prefix
    ///
    /// Format:
    /// ```text
    /// [header: VInt]              ← 2 bits per clustering column (state)
    /// [value_1: type-specific]    ← Only if state is PRESENT (00)
    /// [value_2: type-specific]
    /// ...
    /// ```
    pub(super) fn write_clustering_prefix(
        &mut self,
        clustering_key: &crate::storage::write_engine::mutation::ClusteringKey,
        schema: &TableSchema,
    ) -> Result<()> {
        // Build header: 2 bits per column
        // 00 = PRESENT, 01 = EMPTY, 10 = NULL, 11 = reserved
        let mut header = 0u64;
        for (i, (_, value)) in clustering_key.columns.iter().enumerate() {
            let state = match value {
                Value::Null => 2, // NULL
                _ => 0,           // PRESENT
            };
            header |= (state as u64) << (i * 2);
        }

        // Write header as VUInt
        encode_unsigned(header, &mut self.buffer);

        // Write values for PRESENT columns
        for (i, (_, value)) in clustering_key.columns.iter().enumerate() {
            if !matches!(value, Value::Null) {
                // Get clustering column definition
                if i >= schema.clustering_keys.len() {
                    return Err(Error::Schema(format!(
                        "Clustering key has more columns than schema: {} > {}",
                        i + 1,
                        schema.clustering_keys.len()
                    )));
                }
                let cluster_col = &schema.clustering_keys[i];
                let comparator = ComparatorType::from_data_type(&cluster_col.data_type)?;

                // Write value bytes (type-specific encoding)
                let value_bytes = serialize_value_for_clustering(value, &comparator)?;
                self.buffer.extend_from_slice(&value_bytes);
            }
        }

        Ok(())
    }

    /// Write column bitmap
    ///
    /// Cassandra `Columns.Serializer.serializeSubset()` format.
    ///
    /// For <64 regular columns (the common case), this writes a single
    /// unsigned VInt whose bits indicate **missing** columns:
    ///   - bit = 1 → column is MISSING (NULL / not written)
    ///   - bit = 0 → column is PRESENT
    ///   - bitmap = 0 means all columns present (this case is prevented by
    ///     the caller which sets `HAS_ALL_COLUMNS` instead).
    ///
    /// Only regular columns participate in the bitmap — partition key and
    /// clustering key columns are serialized elsewhere.
    #[cfg(test)]
    pub(super) fn write_column_bitmap(
        &self,
        buf: &mut Vec<u8>,
        mutation: &Mutation,
        schema: &TableSchema,
    ) -> Result<()> {
        // Collect names of columns that are present (non-NULL writes + deletes).
        // Delete operations must be marked as present so the reader parses
        // the tombstone/complex-deletion bytes that write_cells() emits.
        let present_columns: std::collections::HashSet<&str> = mutation
            .operations
            .iter()
            .filter_map(|op| match op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, value }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    value,
                    ..
                } if !matches!(value, Value::Null) => Some(column.as_str()),
                crate::storage::write_engine::mutation::CellOperation::Delete {
                    column, ..
                } => Some(column.as_str()),
                _ => None,
            })
            .collect();

        let regular_columns = self.regular_columns(schema);
        self.write_column_subset(buf, &regular_columns, &present_columns)
    }

    /// Write the columns subset for a merged row's surviving operations.
    ///
    /// Same encoding as [`Self::write_column_bitmap`]; for a pure row
    /// tombstone the ops list is empty, producing the all-missing bitmask.
    pub(super) fn write_merged_column_bitmap(
        &self,
        buf: &mut Vec<u8>,
        row: &RowWrite<'_>,
        schema: &TableSchema,
    ) -> Result<()> {
        use crate::storage::write_engine::mutation::CellOperation;

        let mut present_columns: std::collections::HashSet<&str> = row
            .ops
            .iter()
            .filter_map(|mop| match mop.op {
                CellOperation::Write { column, value }
                | CellOperation::WriteWithTtl { column, value, .. }
                    if !matches!(value, Value::Null) =>
                {
                    Some(column.as_str())
                }
                CellOperation::Delete { column, .. } => Some(column.as_str()),
                _ => None,
            })
            .collect();

        // roborev #885 (Finding 1): a complex column present ONLY via the
        // per-element path (`complex_element_ops`) must also be marked present in
        // the bitmap. `write_complex_element_columns` emits a cell for it
        // (surviving elements and/or a real complex deletion marker), so omitting
        // it from the bitmap would make the reader skip the column entirely or
        // mis-parse the following cell. A `ComplexDeletion`-only column (all
        // elements deleted) still emits a marker, so it counts as present.
        for mop in &row.complex_element_ops {
            match mop.op {
                CellOperation::WriteComplexElement { column, .. }
                | CellOperation::ComplexDeletion { column, .. } => {
                    present_columns.insert(column.as_str());
                }
                _ => {}
            }
        }

        let regular_columns = self.regular_columns(schema);
        self.write_column_subset(buf, &regular_columns, &present_columns)
    }

    /// Get regular (non-PK, non-CK, non-static) columns from schema.
    ///
    /// Cassandra's column bitmap only covers regular columns — partition key
    /// and clustering key columns are serialized separately in the partition
    /// header and clustering prefix. Within the regular set, simple columns
    /// sort before complex columns, then by name.
    pub(super) fn regular_columns<'a>(&self, schema: &'a TableSchema) -> Vec<&'a Column> {
        self.ordered_columns(schema, |column| {
            !column.is_static
                && !schema.is_partition_key(&column.name)
                && !schema.is_clustering_key(&column.name)
        })
    }

    /// Get static columns from schema in Cassandra serialization-header order.
    pub(super) fn static_columns<'a>(&self, schema: &'a TableSchema) -> Vec<&'a Column> {
        self.ordered_columns(schema, |column| column.is_static)
    }

    pub(super) fn ordered_columns<'a, F>(
        &self,
        schema: &'a TableSchema,
        predicate: F,
    ) -> Vec<&'a Column>
    where
        F: Fn(&Column) -> bool,
    {
        let mut columns: Vec<&'a Column> = schema
            .columns
            .iter()
            .filter(|column| predicate(column))
            .collect();
        columns.sort_by_key(|column| column_order_key(column));
        columns
    }

    pub(super) fn write_column_subset(
        &self,
        buf: &mut Vec<u8>,
        columns: &[&Column],
        present_columns: &std::collections::HashSet<&str>,
    ) -> Result<()> {
        let mut present_indices = Vec::new();
        let mut missing_indices = Vec::new();

        for (idx, column) in columns.iter().enumerate() {
            if present_columns.contains(column.name.as_str()) {
                present_indices.push(idx);
            } else {
                missing_indices.push(idx);
            }
        }

        if missing_indices.is_empty() {
            encode_unsigned(0, buf);
            return Ok(());
        }

        if columns.len() < 64 {
            let mut bitmap = 0u64;
            for idx in missing_indices {
                bitmap |= 1u64 << idx;
            }
            encode_unsigned(bitmap, buf);
            return Ok(());
        }

        encode_unsigned((columns.len() - present_indices.len()) as u64, buf);

        if present_indices.len() < columns.len() / 2 {
            for idx in present_indices {
                encode_unsigned(idx as u64, buf);
            }
        } else {
            for idx in missing_indices {
                encode_unsigned(idx as u64, buf);
            }
        }

        Ok(())
    }
}
