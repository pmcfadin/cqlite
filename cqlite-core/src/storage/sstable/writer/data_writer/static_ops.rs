//! Incremental static-column last-write-wins tracker (issue #1668, stage
//! 5c-ii).
//!
//! `collect_static_operations` (`encoding.rs`) used to scan a whole
//! `&[Mutation]` slice in one pass, folding each static-column op into a
//! `best: HashMap<String, StaticMergedOp>` last-write-wins map. That fold is
//! ALREADY incremental in spirit — nothing about it actually needs the whole
//! slice materialized upfront, it just happened to be written as a `for`
//! loop over a slice. This module extracts the SAME per-mutation logic into
//! [`StaticOpsTracker`]: a running tracker with `feed()` (one `Mutation` at a
//! time) and `finish()` (the same `Vec<StaticMergedOp>` the whole-slice
//! version returns), so a FUTURE incremental writer entry point (stage
//! 5c-iv) can feed it one cluster group at a time instead of requiring a
//! fully-materialized `Vec<Mutation>` first.
//!
//! `collect_static_operations` itself is now a THIN WRAPPER over this
//! tracker — same signature, same behavior, byte-identical output — so the
//! CURRENT production call site (`DataWriter::write_partition_with_index_blocks`)
//! is completely unaffected by this stage; only the internal mechanism
//! changed shape.
//!
//! ## Order-dependence note (why `feed()` order must match today's slice order)
//!
//! The last-write-wins tie-break is `>=`, not `>` (see [`StaticOpsTracker::feed`]):
//! at an EXACT timestamp tie, the LATER-fed candidate wins. This is
//! observable, not incidental — `collect_static_operations` iterates
//! `mutations` in whatever order the caller's slice presents them, and
//! `write_partition` sorts that slice by clustering key before this runs. A
//! future incremental caller (5c-iv) MUST feed cluster groups in the SAME
//! order `write_partition`'s sort would have produced for this to remain
//! byte-identical — not resolved here (this stage only proves the TRACKER
//! itself reproduces today's whole-slice result when fed in the SAME order).

use super::*;

/// Running last-write-wins tracker for static-column operations (issue
/// #1668, stage 5c-ii). See the module doc for the order-dependence note.
#[derive(Default)]
pub(crate) struct StaticOpsTracker {
    /// column_name → winning `StaticMergedOp` seen so far.
    best: std::collections::HashMap<String, StaticMergedOp>,
}

impl StaticOpsTracker {
    /// A fresh tracker with no candidates yet.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Fold one mutation's static-column operations into the running
    /// last-write-wins map. Mirrors `collect_static_operations`'s per-
    /// mutation loop body EXACTLY (mutation-level and per-cell shadow-floor
    /// skips, per-cell writetime resolution, `>=` tie-break).
    pub(crate) fn feed(
        &mut self,
        mutation: &Mutation,
        schema: &TableSchema,
        shadow_floor: Option<i64>,
    ) {
        if shadow_floor.is_some_and(|floor| mutation.timestamp_micros <= floor) {
            return;
        }
        for op in &mutation.operations {
            if !is_static_operation(op, schema) {
                continue;
            }
            let col_name = match op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    ..
                }
                | crate::storage::write_engine::mutation::CellOperation::Delete {
                    column, ..
                } => column.clone(),
                // Per-element complex ops are not produced for STATIC complex
                // columns by the current capability — they flow through the
                // regular-row per-element path. Skip them here defensively,
                // matching `collect_static_operations`.
                crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
                    ..
                }
                | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
                    ..
                } => continue,
                crate::storage::write_engine::mutation::CellOperation::DeleteRow => continue,
            };
            let candidate_ts = op_cell_write_timestamp(op, mutation);
            if shadow_floor.is_some_and(|floor| candidate_ts <= floor)
                && matches!(
                    op,
                    crate::storage::write_engine::mutation::CellOperation::Write { .. }
                        | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl { .. }
                        | crate::storage::write_engine::mutation::CellOperation::Delete { .. }
                )
            {
                continue;
            }
            let candidate = StaticMergedOp {
                cell_local_deletion_time: op_cell_local_deletion_time(op, mutation),
                op: op.clone(),
                timestamp_micros: candidate_ts,
                row_ttl_seconds: mutation.ttl_seconds,
            };
            match self.best.entry(col_name) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if candidate.timestamp_micros >= entry.get().timestamp_micros {
                        entry.insert(candidate);
                    }
                }
            }
        }
    }

    /// Consume the tracker and return the resolved static operations, one
    /// per distinct static column, in an unspecified order (the writer sorts
    /// them by schema column order when building the row body) — matches
    /// `collect_static_operations`'s existing contract exactly.
    pub(crate) fn finish(self) -> Vec<StaticMergedOp> {
        self.best.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, KeyColumn};
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
    use crate::types::Value;

    fn schema_with_static() -> TableSchema {
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "region".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
                Column {
                    name: "quota".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        }
    }

    fn static_write(column: &str, value: Value, ts: i64) -> Mutation {
        let table_id = TableId::new("ks", "t");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::Write {
            column: column.to_string(),
            value,
        }];
        Mutation::new(table_id, pk, None, ops, ts, None)
    }

    fn static_op_text(ops: &[StaticMergedOp], column: &str) -> Option<String> {
        ops.iter().find_map(|m| match &m.op {
            CellOperation::Write {
                column: c,
                value: Value::Text(s),
            } if c == column => Some(s.clone()),
            _ => None,
        })
    }

    /// THE proof: feeding mutations ONE AT A TIME via `StaticOpsTracker`
    /// resolves the SAME winner as `collect_static_operations`'s whole-slice
    /// scan, for a fixture with a genuine last-write-wins conflict — checked
    /// against an INDEPENDENTLY reasoned expected winner (the strictly
    /// highest timestamp), not just old-path-equals-new-path.
    #[test]
    fn tracker_matches_whole_slice_for_lww_conflict() {
        let schema = schema_with_static();
        let mutations = vec![
            static_write("region", Value::Text("us-east".to_string()), 100),
            static_write("region", Value::Text("us-west".to_string()), 300), // wins: highest ts
            static_write("region", Value::Text("eu-west".to_string()), 200),
            static_write("quota", Value::Integer(10), 150),
        ];

        // Whole-slice (today's production path, unchanged).
        let whole_slice = collect_static_operations(&mutations, &schema, None);

        // Incremental: fed one mutation at a time, in the SAME arrival order.
        let mut tracker = StaticOpsTracker::new();
        for m in &mutations {
            tracker.feed(m, &schema, None);
        }
        let incremental = tracker.finish();

        // Independent expectation: "us-west" (ts=300) is strictly the
        // highest timestamp among the "region" candidates.
        assert_eq!(
            static_op_text(&whole_slice, "region"),
            Some("us-west".to_string()),
            "whole-slice path must resolve the strictly-highest-timestamp winner"
        );
        assert_eq!(
            static_op_text(&incremental, "region"),
            Some("us-west".to_string()),
            "incremental tracker must resolve the SAME strictly-highest-timestamp winner"
        );
        assert_eq!(
            whole_slice.len(),
            incremental.len(),
            "both paths must resolve the same NUMBER of distinct static columns"
        );
    }

    /// Tie-break proof: at an EXACT timestamp tie, the LATER-fed candidate
    /// wins (the `>=` comparison) — proven independently by construction
    /// (the fixture's SECOND write at the tied timestamp is the one that
    /// must survive), and confirmed identical between the incremental
    /// tracker and the whole-slice path.
    #[test]
    fn tracker_matches_whole_slice_for_exact_timestamp_tie() {
        let schema = schema_with_static();
        let mutations = vec![
            static_write("region", Value::Text("first".to_string()), 500),
            static_write("region", Value::Text("second".to_string()), 500), // later-fed at the SAME ts: wins
        ];

        let whole_slice = collect_static_operations(&mutations, &schema, None);
        let mut tracker = StaticOpsTracker::new();
        for m in &mutations {
            tracker.feed(m, &schema, None);
        }
        let incremental = tracker.finish();

        assert_eq!(
            static_op_text(&whole_slice, "region"),
            Some("second".to_string()),
            "at an exact timestamp tie, the LATER mutation in arrival order must win"
        );
        assert_eq!(
            static_op_text(&incremental, "region"),
            Some("second".to_string()),
            "incremental tracker must resolve the tie identically to the whole-slice path"
        );
    }

    /// Shadow-floor proof: a mutation at or before the partition-tombstone
    /// floor contributes NOTHING, in both paths.
    #[test]
    fn tracker_matches_whole_slice_with_shadow_floor() {
        let schema = schema_with_static();
        let mutations = vec![
            static_write("region", Value::Text("shadowed".to_string()), 100), // <= floor
            static_write("region", Value::Text("survivor".to_string()), 300), // > floor
        ];
        let shadow_floor = Some(200i64);

        let whole_slice = collect_static_operations(&mutations, &schema, shadow_floor);
        let mut tracker = StaticOpsTracker::new();
        for m in &mutations {
            tracker.feed(m, &schema, shadow_floor);
        }
        let incremental = tracker.finish();

        assert_eq!(
            static_op_text(&whole_slice, "region"),
            Some("survivor".to_string())
        );
        assert_eq!(
            static_op_text(&incremental, "region"),
            Some("survivor".to_string()),
            "incremental tracker must apply the SAME shadow-floor gate"
        );
    }

    /// Empty input: both paths resolve to nothing.
    #[test]
    fn tracker_matches_whole_slice_for_empty_input() {
        let schema = schema_with_static();
        let whole_slice = collect_static_operations(&[], &schema, None);
        let tracker = StaticOpsTracker::new();
        let incremental = tracker.finish();
        assert!(whole_slice.is_empty());
        assert!(incremental.is_empty());
    }
}
