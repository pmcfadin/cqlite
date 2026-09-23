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
//! reaches disk. [`SelectExecutor::execute`] (`execute.rs`) and
//! [`SelectExecutor::execute_streaming`] (`mod.rs`) both call
//! [`SelectExecutor::raw_view_base_name`] immediately after extracting the
//! FROM-clause table id, BEFORE the normal `resolve_table_schema` /
//! execution-step pipeline runs, and route a recognized suffix entirely to
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
use crate::query::result::{QueryMetadata, QueryResult, QueryRow};
use crate::query::result_budget::enforce_materialized_rows;
use crate::query::select_ast::{SelectClause, SelectExpression};
use crate::query::select_optimizer::{OptimizedQueryPlan, SSTablePredicate};
use crate::schema::TableSchema;
use crate::types::Value;
use crate::{Error, Result, TableId};
use point::raw_view_point_rows;
use scan::raw_view_full_scan_rows;
use std::collections::HashSet;
use std::sync::Arc;

/// `true` when `row`'s `row_kind` column is the plain `'row'` case (a live
/// or tombstoned DATA row) rather than a synthetic `partition_tombstone` /
/// `range_tombstone_start` / `range_tombstone_end` row, which carries no
/// clustering columns (or, for a partition-tombstone row, no data columns at
/// all).
pub(super) fn is_plain_data_row(row: &QueryRow) -> bool {
    row.values.get("row_kind") == Some(&Value::text("row"))
}

/// Split `predicates` into the subset naming a PARTITION-KEY column (or a
/// `token(...)` predicate, which constrains the partition key too) and
/// everything else (clustering-key / regular-column predicates).
///
/// Used by both row producers to apply the CORRECT predicate backstop
/// per row-kind (roborev finding, issue #4222): a synthetic
/// `partition_tombstone`/`range_tombstone_*` row carries no clustering/
/// regular columns to test a clustering/regular predicate against, and the
/// spec requires it to stay visible "even when the generation holds no live
/// rows" — but it DOES carry partition-key values (`insert_pk_values`), so a
/// partition-key predicate (e.g. a composite key's `pk1 = 1` alone, which
/// `classify_partition_lookup` cannot push down as a full targeted lookup)
/// must still apply to it, or a full scan filtered to one partition would
/// wrongly return every OTHER partition's tombstone rows too.
pub(super) fn split_predicates_by_key_role<'a>(
    predicates: &'a [SSTablePredicate],
    base_schema: &TableSchema,
) -> (Vec<&'a SSTablePredicate>, Vec<&'a SSTablePredicate>) {
    let pk_names: HashSet<&str> = base_schema
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    predicates
        .iter()
        .partition(|p| p.is_token() || pk_names.contains(p.column.as_str()))
}

/// Apply the raw view's post-scan predicate backstop to one row: partition-
/// key predicates apply UNCONDITIONALLY (every row carries the partition key,
/// synthetic or not); clustering/regular-column predicates apply ONLY to a
/// plain data row (`row_kind = 'row'`) — a synthetic row is exempt from
/// those (roborev finding, issue #4222).
pub(super) fn row_passes_predicates(
    row: &QueryRow,
    pk_predicates: &[&SSTablePredicate],
    other_predicates: &[&SSTablePredicate],
) -> Result<bool> {
    for p in pk_predicates {
        if super::evaluate_leaf(row, p) != super::LeafOutcome::True {
            return Ok(false);
        }
    }
    if is_plain_data_row(row) {
        for p in other_predicates {
            if super::evaluate_leaf(row, p) != super::LeafOutcome::True {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

impl super::SelectExecutor {
    /// Decide whether `table_id` should be intercepted as a raw-view
    /// reference, returning the (owned) base table name when so.
    ///
    /// A LITERAL table actually named with the `_raw_sstable_data` suffix
    /// takes precedence (roborev finding, issue #4222): unconditional
    /// stripping made such a real table permanently unreachable through
    /// this executor. Only when the FULL, unstripped name resolves to NO
    /// registered schema does the suffix count as the D1 naming convention.
    /// Checked on the bare table name only (no keyspace segment), matching
    /// how the suffix is a convention over the flat `keyspace.table`
    /// namespace.
    pub(super) async fn raw_view_base_name(&self, table_id: &TableId) -> Option<String> {
        let (_, bare_table_name) = parse_table_id(table_id);
        let base = strip_raw_view_suffix(&bare_table_name)?;
        if self.resolve_table_schema(table_id).await.is_some() {
            // A real table is literally registered under the suffixed name —
            // let it resolve normally, never shadow it.
            return None;
        }
        Some(base.to_string())
    }

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
        // Fail closed on every reshaping clause this slice does not implement
        // (roborev finding, issue #4222): ORDER BY needs a real sort over the
        // producers' unordered output, DISTINCT/aggregates need dedup/fold
        // machinery — none of which this view builds (design.md D4's
        // JOIN-gap precedent: a general query-engine capability, not a
        // raw-view concern). Checked BEFORE any producer work runs, so a
        // rejected query never pays for a scan it will discard.
        if plan.aggregation_plan.is_some() {
            return Err(Error::unsupported_query(
                "aggregate functions are not supported over a _raw_sstable_data view",
            ));
        }
        if matches!(plan.statement.select_clause, SelectClause::Distinct(_)) {
            return Err(Error::unsupported_query(
                "SELECT DISTINCT is not supported over a _raw_sstable_data view",
            ));
        }
        if plan.statement.order_by.is_some() {
            return Err(Error::unsupported_query(
                "ORDER BY is not supported over a _raw_sstable_data view",
            ));
        }

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
            keyspace
                .as_deref()
                .map(|k| format!("{k}."))
                .unwrap_or_default(),
            base_name
        ));

        let columns = raw_view_columns(&base_schema)?;
        let readers = self
            .resolve_raw_view_readers(&base_table_id, keyspace.is_some())
            .await?;

        // Split the predicate set ONCE (roborev finding, issue #4222): a
        // partition-key predicate (or `token(...)`) applies to EVERY row,
        // synthetic or not (a synthetic row still carries the partition key);
        // a clustering/regular-column predicate applies ONLY to a plain data
        // row (`row_kind = 'row'`) — a synthetic `partition_tombstone`/
        // `range_tombstone_*` row carries no such columns to test, and the
        // spec requires it to stay visible "even when the generation holds
        // no live rows". Threaded into BOTH producers so the filtering
        // happens where LIMIT can stop the walk early, not after the whole
        // corpus is already materialized.
        let (pk_predicates, other_predicates) =
            split_predicates_by_key_role(&plan.sstable_predicates, &base_schema);

        // LIMIT/OFFSET (roborev finding, issue #4222): the raw view returns
        // straight from `execute_raw_sstable_view`, never reaching the
        // execution-step pipeline's `Limit` step. `stop_after` tells the
        // FULL-SCAN producer to stop once this many ACCEPTED (post-predicate)
        // rows are collected — mirroring issue #1577's LIMIT-pushdown intent —
        // so `SELECT * FROM big_raw_sstable_data LIMIT 1` does not decode the
        // whole corpus, and does not spuriously trip `max_result_rows` on a
        // table that exceeds it (issue #1578's explicit-LIMIT exemption,
        // applied at the SOURCE this time, not just the final check below).
        let offset = plan.statement.offset.unwrap_or(0) as usize;
        let limit = plan
            .statement
            .limit
            .as_ref()
            .map(|l| l.count as usize)
            .unwrap_or(usize::MAX);
        let stop_after = plan
            .statement
            .limit
            .as_ref()
            .map(|_| offset.saturating_add(limit));

        // Reuse the SAME sstable-predicate extraction the optimizer already
        // ran for this statement's WHERE clause (predicates are plain
        // column=value facts, independent of which table string the SELECT
        // named) to decide whether this is a partition-targeted point read
        // or an unbounded full scan (spec: "pushed to the point-read path,
        // never a scan" for a full `WHERE pk = ?`).
        let outcome = classify_partition_lookup(&plan.sstable_predicates, Some(&base_schema));

        let rows = match outcome {
            PartitionLookupOutcome::Targeted(pk_bytes) => {
                raw_view_point_rows(
                    &readers,
                    &base_schema,
                    std::slice::from_ref(&pk_bytes),
                    &pk_predicates,
                    &other_predicates,
                )
                .await?
            }
            PartitionLookupOutcome::MultiTargeted(pk_keys) => {
                raw_view_point_rows(
                    &readers,
                    &base_schema,
                    &pk_keys,
                    &pk_predicates,
                    &other_predicates,
                )
                .await?
            }
            PartitionLookupOutcome::Fallback(_) => {
                raw_view_full_scan_rows(
                    &readers,
                    &base_schema,
                    &pk_predicates,
                    &other_predicates,
                    self.max_result_bytes,
                    self.max_result_rows,
                    stop_after,
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
                // Look up every selected name BEFORE enumerating (roborev
                // finding, issue #4222): assigning `position` from the
                // pre-filter index left a `filter_map`'d-out unknown column
                // a GAP in the surviving positions (`SELECT pk, bogus, ck`
                // produced positions `0, 2`), violating the dense/ordered
                // invariant `metadata.columns` must hold. An unknown column
                // now fails closed (D8) instead of being silently dropped.
                let mut trimmed_columns = Vec::with_capacity(selected.len());
                for name in &selected {
                    let col = columns
                        .iter()
                        .find(|c| c.name == *name)
                        .cloned()
                        .ok_or_else(|| {
                            Error::Schema(format!(
                                "raw SSTable view: SELECT names unknown column '{name}' — \
                                 not part of the raw view's column contract"
                            ))
                        })?;
                    trimmed_columns.push(col);
                }
                for (idx, c) in trimmed_columns.iter_mut().enumerate() {
                    c.position = idx;
                }
                (trimmed_rows, trimmed_columns)
            }
            _ => (rows, columns),
        };

        // Apply the SAME LIMIT/OFFSET the producer was told to `stop_after`
        // (mirroring the constant-query branch, `execute.rs`'s `SELECT 1`
        // handling): the producer stopped once `offset + limit` ACCEPTED
        // rows were collected (or ran out of corpus first), so this trims
        // exactly `offset` off the front and caps at `limit` — never fewer
        // than available, never more than requested.
        let rows: Vec<_> = rows.into_iter().skip(offset).take(limit).collect();

        // Same final budget check every other query path applies (issue
        // #1582/D6) — belt-and-braces alongside the full-scan producer's own
        // incremental check (which never materializes past the budget); the
        // point-key path has no incremental check of its own, so this is its
        // ONLY enforcement. Applied to the POST-limit rows, so a `LIMIT 10`
        // raw-view query is never penalized for the corpus it did not keep.
        let effective_max_rows = if plan.statement.limit.is_some() {
            usize::MAX
        } else {
            self.max_result_rows
        };
        enforce_materialized_rows(&rows, self.max_result_bytes, effective_max_rows)?;

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

    /// Resolve the per-generation reader snapshot for `base_table_id`,
    /// refusing a QUALIFIED (keyspace-carrying) raw-view request that only
    /// resolved via the reader map's bare-table-name FALLBACK (roborev
    /// finding, issue #4222).
    ///
    /// `resolve_reader_list` falls back to a bare-name match when no exact
    /// `keyspace.table` key exists; `manager_point_read.rs` threads that same
    /// `fully_qualified_match` signal into `get_with_resolution_unmetered`
    /// specifically so a qualified query never silently reads a DIFFERENT
    /// keyspace's same-named table (#1321) — this view had no equivalent
    /// guard, so `ks_b.t_raw_sstable_data` could read `ks_a.t`'s rows tagged
    /// as `ks_b.t`'s. `was_qualified` is `false` for an unqualified request
    /// (no keyspace to mismatch, matching `fully_qualified_match`'s own
    /// contract), so this never refuses a legitimately bare table name.
    async fn resolve_raw_view_readers(
        &self,
        base_table_id: &TableId,
        was_qualified: bool,
    ) -> Result<Vec<Arc<crate::storage::sstable::reader::SSTableReader>>> {
        let (readers, fully_qualified_match) =
            self.storage.raw_view_reader_snapshot(base_table_id).await;
        if was_qualified && !fully_qualified_match {
            return Err(Error::Table(format!(
                "raw SSTable view: '{base_table_id}' resolved only via a bare-table-name \
                 fallback, not an exact keyspace match — refusing to read a possibly \
                 DIFFERENT keyspace's same-named table (issue #1321's guard, applied here \
                 for issue #4222)"
            )));
        }
        Ok(readers)
    }
}
