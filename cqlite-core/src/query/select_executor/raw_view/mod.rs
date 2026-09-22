//! The raw SSTable view — `<keyspace>.<table>_raw_sstable_data` (issue #4222,
//! `openspec/changes/raw-sstable-view/design.md`).
//!
//! Exposes every physical row CQLite has already decoded for a table,
//! UNRECONCILED, one row per physical row per SSTable generation, with
//! per-cell/row/partition metadata and source SSTable identity — the raw
//! facts a reconciled `SELECT` on the base table discards.
//!
//! ## Interception point (design.md D6)
//!
//! Since the query engine has no catalog/`TableProvider` registry at all
//! (table resolution runs a `TableId` string straight to on-disk SSTables),
//! recognizing this new virtual table means intercepting resolution BEFORE it
//! reaches disk. [`SelectExecutor::execute`] (`execute.rs`) checks the FROM
//! table name for [`strip_raw_view_suffix`]'s suffix immediately after
//! extracting it, BEFORE the normal `resolve_table_schema` /
//! execution-step pipeline runs, and routes entirely to
//! [`SelectExecutor::execute_raw_sstable_view`] — never through
//! `StorageEngine::scan`/`scan_partition`/`SSTableManager::scan_with_meter`,
//! which RECONCILE across generations (design.md D5's "why not `KWayMerger`").
//!
//! ## Fail-closed (design.md D8)
//!
//! Unlike the base SELECT path (which silently returns zero rows for an
//! unknown table and silently falls back to row-derived columns for a missing
//! schema), an unresolvable BASE table/schema here raises a typed
//! `Error::Table`/`Error::Schema` — this is new surface, built fail-closed
//! from the start, and the base table's own permissive behavior is untouched.

mod columns;
mod point;
mod row_map;
mod scan;

pub(super) use columns::{raw_view_columns, strip_raw_view_suffix};

use super::{classify_partition_lookup, parse_table_id, PartitionLookupOutcome};
use crate::query::result::{QueryMetadata, QueryResult};
use crate::query::result_budget::enforce_materialized_rows;
use crate::query::select_ast::{SelectClause, SelectExpression};
use crate::query::select_optimizer::OptimizedQueryPlan;
use crate::schema::TableSchema;
use crate::{Error, Result, TableId};
use point::raw_view_point_rows;
use scan::raw_view_full_scan_rows;

impl super::SelectExecutor {
    /// Entry point for a `_raw_sstable_data` table reference (design.md D6).
    /// `table_id` carries the RAW-VIEW name (suffix included, as the FROM
    /// clause named it — needed to resolve the reader snapshot and to keep
    /// the keyspace); `base_name` is the already-stripped base table name.
    pub(super) async fn execute_raw_sstable_view(
        &self,
        plan: &OptimizedQueryPlan,
        table_id: &TableId,
        base_name: &str,
    ) -> Result<QueryResult> {
        let (keyspace, _) = parse_table_id(table_id);
        let base_schema = self
            .resolve_base_schema_for_raw_view(keyspace.as_deref(), base_name)
            .await?;

        // The reader map (`SSTableManager::table_readers`) is keyed by the
        // BASE table's own name — the raw-view suffix is a query-engine-side
        // naming convention only (design.md D1) and is never itself a
        // registered table identity. Every reader-snapshot lookup below MUST
        // use this, never `table_id` (which still carries the suffix).
        let base_table_id = TableId::new(format!(
            "{}{}",
            keyspace.as_deref().map(|k| format!("{k}.")).unwrap_or_default(),
            base_name
        ));

        let columns = raw_view_columns(&base_schema);

        // Reuse the SAME sstable-predicate extraction the optimizer already
        // ran for this statement's WHERE clause (predicates are plain
        // column=value facts, independent of which table string the SELECT
        // named) to decide whether this is a partition-targeted point read
        // or an unbounded full scan (spec: "pushed to the point-read path,
        // never a scan" for a full `WHERE pk = ?`).
        let outcome =
            classify_partition_lookup(&plan.sstable_predicates, Some(&base_schema));

        let rows = match outcome {
            PartitionLookupOutcome::Targeted(pk_bytes) => {
                let readers = self.storage.raw_view_reader_snapshot(&base_table_id).await;
                raw_view_point_rows(&readers, &base_schema, std::slice::from_ref(&pk_bytes))
                    .await?
            }
            PartitionLookupOutcome::MultiTargeted(pk_keys) => {
                let readers = self.storage.raw_view_reader_snapshot(&base_table_id).await;
                raw_view_point_rows(&readers, &base_schema, &pk_keys).await?
            }
            PartitionLookupOutcome::Fallback(_) => {
                let readers = self.storage.raw_view_reader_snapshot(&base_table_id).await;
                raw_view_full_scan_rows(
                    &readers,
                    &base_schema,
                    self.max_result_bytes,
                    self.max_result_rows,
                )
                .await?
            }
        };

        // Plain-column projection trimming (`SELECT a, b, ...`), reusing the
        // SAME `trim_projection` the base pipeline's `Project` step uses so
        // the two never drift. `SELECT *` and anything reshaping (DISTINCT,
        // aggregates, expressions, WRITETIME/TTL) are OUT OF SCOPE for this
        // slice (design.md D4's JOIN-gap precedent: DISTINCT/aggregation over
        // the raw view is a general query-engine capability, not a raw-view
        // concern) and return every column unfiltered — a known, documented
        // limitation rather than a silent wrong answer, since every column
        // this contract defines is still present and correctly valued.
        let (rows, columns) = match &plan.statement.select_clause {
            SelectClause::Columns(exprs)
                if exprs
                    .iter()
                    .all(|e| matches!(e, SelectExpression::Column(_))) =>
            {
                let selected: Vec<&str> = exprs
                    .iter()
                    .filter_map(|e| match e {
                        SelectExpression::Column(c) => Some(c.column.as_str()),
                        _ => None,
                    })
                    .collect();
                let trimmed_rows = self.trim_projection(rows, exprs);
                let trimmed_columns: Vec<_> = selected
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, name)| {
                        columns.iter().find(|c| c.name == *name).map(|c| {
                            let mut c = c.clone();
                            c.position = idx;
                            c
                        })
                    })
                    .collect();
                (trimmed_rows, trimmed_columns)
            }
            _ => (rows, columns),
        };

        // Same final budget check every other query path applies (issue
        // #1582/D6) — belt-and-braces alongside the full-scan producer's own
        // incremental check (which never materializes past the budget); the
        // point-key path has no incremental check of its own, so this is its
        // ONLY enforcement.
        enforce_materialized_rows(&rows, self.max_result_bytes, self.max_result_rows)?;

        let total_rows = rows.len() as u64;
        Ok(QueryResult {
            rows,
            rows_affected: total_rows,
            execution_time_ms: 0,
            metadata: QueryMetadata {
                columns,
                total_rows: Some(total_rows),
                plan_info: None,
                performance: Default::default(),
                warnings: vec![],
                access_path: None,
            },
        })
    }

    /// Resolve the BASE table's schema for a raw-view request, FAILING CLOSED
    /// (design.md D8) — never falling back to row-derived columns the way
    /// [`SelectExecutor::resolve_table_schema`]'s best-effort `Option` does.
    async fn resolve_base_schema_for_raw_view(
        &self,
        keyspace: Option<&str>,
        base_name: &str,
    ) -> Result<TableSchema> {
        let keyspace_opt = keyspace.map(|s| s.to_string());
        let qualified = format!(
            "{}{}",
            keyspace.map(|k| format!("{k}.")).unwrap_or_default(),
            base_name
        );
        self._schema
            .find_schema_by_table(&keyspace_opt, base_name)
            .await
            .map_err(|e| {
                Error::Schema(format!(
                    "raw SSTable view: could not resolve schema for base table \
                     '{qualified}': {e}"
                ))
            })?
            .ok_or_else(|| {
                Error::Table(format!(
                    "raw SSTable view: base table '{qualified}' does not exist or has \
                     no resolvable schema (queried as '{qualified}{}')",
                    columns::RAW_VIEW_SUFFIX,
                ))
            })
    }
}
