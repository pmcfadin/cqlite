//! Schema-constant ordered column lists, memoized once per writer (issue #1674,
//! R3).
//!
//! Part of the `data_writer` responsibility split (issue #1118): this module
//! holds one `impl DataWriter` block. `use super::*` pulls the shared writer
//! types, serialization/schema helpers, and crate imports re-exported from
//! `data_writer/mod.rs`. No emitted bytes change.
//!
//! The schema is fixed for a writer's lifetime, yet `regular_columns` /
//! `static_columns` were re-filtered and re-sorted up to 3× per row, and the
//! sort comparator (`column_order_key` → `is_complex_column`) allocated a
//! lowercased `String` on every comparison. These accessors resolve through a
//! lazily-built [`OrderedCols`](super::OrderedCols) cache instead, so the sort +
//! `to_lowercase` classification runs exactly once (O(C)), never per row.

use super::*;

impl DataWriter {
    /// Get regular (non-PK, non-CK, non-static) columns from schema.
    ///
    /// Cassandra's column bitmap only covers regular columns — partition key
    /// and clustering key columns are serialized separately in the partition
    /// header and clustering prefix. Within the regular set, simple columns
    /// sort before complex columns, then by name. The order is served from the
    /// per-writer cache (issue #1674, R3), so no per-row sort runs.
    pub(super) fn regular_columns<'a>(&self, schema: &'a TableSchema) -> Vec<&'a Column> {
        self.cached_cols(schema)
            .regular
            .iter()
            .map(|&idx| &schema.columns[idx])
            .collect()
    }

    /// Get static columns from schema in Cassandra serialization-header order.
    pub(super) fn static_columns<'a>(&self, schema: &'a TableSchema) -> Vec<&'a Column> {
        self.cached_cols(schema)
            .static_
            .iter()
            .map(|&idx| &schema.columns[idx])
            .collect()
    }

    /// Regular columns paired with their cached `is_complex` flag, in Cassandra
    /// serialization-header order (issue #1674, R3). Lets the cell-emission loop
    /// read complexity from the cache instead of re-lowercasing each column's
    /// type per row.
    pub(super) fn regular_columns_with_complex<'a>(
        &self,
        schema: &'a TableSchema,
    ) -> Vec<(&'a Column, bool)> {
        let cache = self.cached_cols(schema);
        cache
            .regular
            .iter()
            .map(|&idx| (&schema.columns[idx], cache.is_complex[idx]))
            .collect()
    }

    /// `is_complex_column` for the column named `col_name`, read from the
    /// per-writer cache (issue #1674, R3) so `to_lowercase` never runs per row.
    /// Unknown names (no matching schema column) are non-complex, matching the
    /// prior `find(...).map(...).unwrap_or(false)` behaviour.
    pub(super) fn column_is_complex(&self, schema: &TableSchema, col_name: &str) -> bool {
        schema
            .columns
            .iter()
            .position(|c| c.name == col_name)
            .map(|idx| self.cached_cols(schema).is_complex[idx])
            .unwrap_or(false)
    }

    /// Borrow the schema-constant ordered column cache, building it on first use
    /// (issue #1674, R3). The schema is fixed for the writer's lifetime, so this
    /// runs the filter + sort + `is_complex_column` classification exactly ONCE;
    /// every later row read is `O(C)` index resolution with no `to_lowercase`.
    ///
    /// Invariant: a `DataWriter` sees exactly ONE schema for its lifetime
    /// (`SSTableWriter` owns one `schema` and threads it into every call). The
    /// cache is built from the FIRST schema and reused; since `is_complex` /
    /// index resolution below assumes the passed `schema` matches, the
    /// `debug_assert` fails loudly in debug/test if a caller ever swaps schemas
    /// (a differently-sized column list) rather than emitting wrong bytes.
    pub(super) fn cached_cols(&self, schema: &TableSchema) -> &OrderedCols {
        let cols = self
            .column_cache
            .get_or_init(|| Self::build_ordered_cols(schema));
        debug_assert_eq!(
            cols.is_complex.len(),
            schema.columns.len(),
            "DataWriter column cache built from a different schema than now passed \
             (one-schema-per-writer invariant, issue #1674)"
        );
        cols
    }

    /// Compute the ordered regular/static column index lists + per-column
    /// `is_complex` classification (issue #1674, R3).
    ///
    /// Byte-order invariant: the ordering MUST equal the previous
    /// `sort_by_key(column_order_key)` — i.e. the same `(is_complex, name)` tuple
    /// key — so the emitted column bitmap / cell order stays byte-identical. Names
    /// are unique within a table, so ties on the key never occur and sort
    /// stability is irrelevant.
    fn build_ordered_cols(schema: &TableSchema) -> OrderedCols {
        // One `to_lowercase` per column (O(C)), never per row.
        let is_complex: Vec<bool> = schema
            .columns
            .iter()
            .map(|column| is_complex_column(&column.data_type))
            .collect();
        // Same key as `column_order_key(column)` = `(is_complex, name)`, resolved
        // through the precomputed `is_complex` slice.
        let order_key = |idx: usize| (is_complex[idx], schema.columns[idx].name.as_str());

        let mut regular: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| {
                !column.is_static
                    && !schema.is_partition_key(&column.name)
                    && !schema.is_clustering_key(&column.name)
            })
            .map(|(idx, _)| idx)
            .collect();
        regular.sort_by(|&a, &b| order_key(a).cmp(&order_key(b)));

        let mut static_: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.is_static)
            .map(|(idx, _)| idx)
            .collect();
        static_.sort_by(|&a, &b| order_key(a).cmp(&order_key(b)));

        OrderedCols {
            regular,
            static_,
            is_complex,
        }
    }
}

/// A thread-local recording scope for `is_complex_column` calls on the WRITE
/// path (issue #1674, R3), mirroring the `#2428` parallel-test-pollution-immune
/// design of the `work_counters` scopes (`cell_data_clone_scope` et al.). The
/// writer's `is_complex_column` allocates a lowercased `String` per call, so this
/// scope makes it observable that the schema-constant ordered column lists and
/// per-column complexity classification are computed ONCE per writer (an `O(C)`
/// pass) rather than re-derived per row (`O(R·C·log C)`).
///
/// Kept HERE, alongside the cache it guards, rather than in the (already
/// over-threshold, epic #1116) `work_counters.rs`. `#[cfg(test)]`: the
/// `record()` call in `is_complex_column` is likewise `#[cfg(test)]`-gated, so
/// production pays ZERO added cost. Also gated on `feature = "write-support"`
/// because `is_complex_column` and this module only compile with that feature.
#[cfg(all(test, feature = "write-support"))]
pub(crate) mod is_complex_scope {
    use std::cell::Cell;

    thread_local! {
        /// `Some(count)` while an [`IsComplexScope`] is active on this thread,
        /// `None` otherwise. Only `is_complex_column` calls on this thread bump it.
        static SCOPED: Cell<Option<u64>> = const { Cell::new(None) };
    }

    /// Bump the active scope on the current thread, if any. A no-op on threads
    /// (production writes, other tests) with no active scope.
    pub(crate) fn record() {
        SCOPED.with(|c| {
            if let Some(v) = c.get() {
                c.set(Some(v.saturating_add(1)));
            }
        });
    }

    /// A per-thread recording scope for `is_complex_column`. Open one before
    /// driving row writes and read [`count`](IsComplexScope::count) after. Immune
    /// to concurrent tests on other threads (issue #2428); dropping it clears the
    /// scope. Deliberately `!Send` (holds a `PhantomData<*const ()>`).
    pub(crate) struct IsComplexScope {
        _not_send: std::marker::PhantomData<*const ()>,
    }

    impl IsComplexScope {
        /// Begin recording on the current thread. Panics if a scope is already
        /// active on this thread (one scope per assertion; nesting unsupported).
        pub(crate) fn new() -> Self {
            SCOPED.with(|c| {
                assert!(
                    c.get().is_none(),
                    "an IsComplexScope is already active on this thread (nesting unsupported)"
                );
                c.set(Some(0));
            });
            Self {
                _not_send: std::marker::PhantomData,
            }
        }

        /// `is_complex_column` increments recorded on this thread since the scope
        /// opened.
        pub(crate) fn count(&self) -> u64 {
            SCOPED.with(|c| c.get().unwrap_or(0))
        }
    }

    impl Drop for IsComplexScope {
        fn drop(&mut self) {
            SCOPED.with(|c| c.set(None));
        }
    }
}
