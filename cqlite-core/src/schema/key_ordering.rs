//! Ordered partition/clustering key accessors for [`TableSchema`] (issue #1677).
//!
//! Extracted from `schema/mod.rs` (source-split doctrine, issue #1116) so the
//! per-call re-sort that finding R6 flagged can be removed without growing the
//! already-over-threshold `mod.rs`.
//!
//! [`TableSchema::ordered_partition_keys`] / [`TableSchema::ordered_clustering_keys`]
//! previously sorted their key slice by `position` on **every** call. During
//! CQL→mutation building these are hit once per statement, so a `BEGIN BATCH` of N
//! statements re-sorted N times even though the schema — and hence the ordering —
//! is fixed. Key `position`s are assigned contiguously in order at schema
//! construction (see `TableSchema::from_sstable_header`), so the slice is virtually
//! always already sorted; the fast path below detects that with one linear scan and
//! skips the sort entirely. When a schema *is* built out of order the sort still
//! runs, so the returned order is byte-for-byte identical to before.

use super::{ClusteringColumn, KeyColumn, TableSchema};

impl TableSchema {
    /// Get partition key columns ordered by `position`.
    ///
    /// Returns the same order as a full `sort_by_key(position)` would, but skips
    /// the sort when the keys are already in ascending `position` order (the
    /// common case — see the module docs), so it is no longer re-sorted per call.
    pub fn ordered_partition_keys(&self) -> Vec<&KeyColumn> {
        let mut keys = self.partition_keys.iter().collect::<Vec<_>>();
        if !keys.is_sorted_by_key(|k| k.position) {
            keys.sort_by_key(|k| k.position);
        }
        keys
    }

    /// Get clustering key columns ordered by `position`.
    ///
    /// Same order as before; the sort is skipped when the keys are already ordered.
    pub fn ordered_clustering_keys(&self) -> Vec<&ClusteringColumn> {
        let mut keys = self.clustering_keys.iter().collect::<Vec<_>>();
        if !keys.is_sorted_by_key(|k| k.position) {
            keys.sort_by_key(|k| k.position);
        }
        keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, KeyColumn};
    use std::collections::HashMap;

    fn schema_with(
        partition_keys: Vec<KeyColumn>,
        clustering_keys: Vec<ClusteringColumn>,
    ) -> TableSchema {
        TableSchema {
            keyspace: "ks".into(),
            table: "t".into(),
            partition_keys,
            clustering_keys,
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn pk(name: &str, position: usize) -> KeyColumn {
        KeyColumn {
            name: name.into(),
            data_type: "int".into(),
            position,
        }
    }

    fn ck(name: &str, position: usize) -> ClusteringColumn {
        ClusteringColumn {
            name: name.into(),
            data_type: "int".into(),
            position,
            order: ClusteringOrder::Asc,
        }
    }

    #[test]
    fn already_sorted_keys_keep_their_order() {
        let schema = schema_with(
            vec![pk("a", 0), pk("b", 1), pk("c", 2)],
            vec![ck("x", 0), ck("y", 1)],
        );
        let pks: Vec<_> = schema
            .ordered_partition_keys()
            .iter()
            .map(|k| k.name.clone())
            .collect();
        assert_eq!(pks, vec!["a", "b", "c"]);
        let cks: Vec<_> = schema
            .ordered_clustering_keys()
            .iter()
            .map(|k| k.name.clone())
            .collect();
        assert_eq!(cks, vec!["x", "y"]);
    }

    #[test]
    fn out_of_order_keys_are_still_sorted_by_position() {
        // Byte-identical to the previous unconditional sort: order is by position,
        // not by insertion order.
        let schema = schema_with(
            vec![pk("c", 2), pk("a", 0), pk("b", 1)],
            vec![ck("y", 1), ck("x", 0)],
        );
        let pks: Vec<_> = schema
            .ordered_partition_keys()
            .iter()
            .map(|k| (k.name.clone(), k.position))
            .collect();
        assert_eq!(pks, vec![("a".into(), 0), ("b".into(), 1), ("c".into(), 2)]);
        let cks: Vec<_> = schema
            .ordered_clustering_keys()
            .iter()
            .map(|k| (k.name.clone(), k.position))
            .collect();
        assert_eq!(cks, vec![("x".into(), 0), ("y".into(), 1)]);
    }

    #[test]
    fn empty_key_lists_are_handled() {
        let schema = schema_with(vec![], vec![]);
        assert!(schema.ordered_partition_keys().is_empty());
        assert!(schema.ordered_clustering_keys().is_empty());
    }
}
