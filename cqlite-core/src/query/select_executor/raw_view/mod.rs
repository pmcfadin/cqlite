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
use crate::query::select_ast::{SelectClause, SelectExpression, WhereExpression};
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

/// Count every `Comparison` LEAF in a PUSHABLE position (through `And`/
/// `Parentheses`), or `None` if the tree contains an `Or`/`Not` ANYWHERE —
/// mirroring exactly what `select_optimizer.rs::collect_sstable_predicates`
/// walks.
///
/// Used to detect when the WHERE clause was only PARTIALLY captured by
/// `plan.sstable_predicates` (roborev finding, issue #4222 — round 2 of this
/// same class): `Or`/`Not` are not the only way a comparison silently fails
/// to lower. `select_optimizer.rs::column_comparison_to_predicate` returns
/// `None` — a silent drop even in a perfectly pushable `And`/top-level
/// position — for `!=`, `NotIn`, `Like`, `NotLike`, `NotBetween`, `IsNull`,
/// `IsNotNull`, `Regex`, and any comparison whose right-hand side is not a
/// literal (e.g. a column-to-column comparison). Each SUCCESSFULLY-lowered
/// leaf contributes EXACTLY one `SSTablePredicate`
/// (`column_comparison_to_predicate`'s every `Some` arm), so comparing this
/// count against `plan.sstable_predicates.len()` (once `Or`/`Not` — which
/// would make this `None` — is ruled out) detects ANY dropped leaf, not
/// just the OR/NOT shape: `pk = 1 AND val != 'x'` yields the NON-empty
/// predicate list `[pk = 1]` (1 predicate, 2 leaves) — checking
/// "predicates is empty" alone would miss it.
fn count_pushable_comparison_leaves(expr: &WhereExpression) -> Option<usize> {
    match expr {
        WhereExpression::Comparison(_) => Some(1),
        WhereExpression::Or(_) | WhereExpression::Not(_) => None,
        WhereExpression::And(exprs) => {
            let mut total = 0usize;
            for e in exprs {
                total += count_pushable_comparison_leaves(e)?;
            }
            Some(total)
        }
        WhereExpression::Parentheses(inner) => count_pushable_comparison_leaves(inner),
    }
}

/// Every column name that is present, with a REAL (non-synthesized) value,
/// on EVERY physical row this view emits regardless of `row_kind` — the
/// partition key (handled separately, see below), the source-identity quad,
/// `row_kind` itself, and the partition-/range-deletion columns.
///
/// A predicate naming one of these must be applied to EVERY row, synthetic
/// or not (roborev finding, issue #4222 — round 5 of this same class): the
/// prior split exempted ALL non-partition-key predicates from a synthetic
/// row, which wrongly ALSO exempted `generation`/`sstable`/`format`/
/// `position`/`row_kind` and the partition-/range-deletion columns — so
/// `WHERE pk = 2 AND generation = 1` incorrectly included a gen-2
/// partition-tombstone row (`generation` IS present and real on that row;
/// it was just never CHECKED). `partition_deletion_time`/
/// `partition_deletion_timestamp`/`bound_inclusive`/`range_deletion_time`/
/// `range_deletion_timestamp` are only ever POPULATED on their own specific
/// synthetic row_kind (never on a plain `row`), but a predicate against them
/// is still meaningful there and must not be silently skipped either — this
/// set, not `is_plain_data_row`, is what decides "always apply".
///
/// Deliberately NOT in this set (see [`row_passes_predicates`]): the
/// clustering-key / regular-column values and their `_timestamp`/`_ttl`/
/// `_local_deletion_time`/`_tombstone`/`_complex_deletion*` metadata
/// derivatives, and the ROW-level `row_timestamp`/`row_ttl`/
/// `row_local_deletion_time`/`row_tombstone`/`row_deletion_timestamp`
/// quintet — every one of those is populated ONLY on a plain `row_kind =
/// 'row'` row (`row_map.rs`'s `Live`/`Tombstone` arms), so they stay exempt
/// for a synthetic row, matching the spec's "even when the generation holds
/// no live rows" requirement for `partition_tombstone`/`range_tombstone_*`.
fn always_applicable_column_names() -> &'static HashSet<&'static str> {
    static NAMES: std::sync::OnceLock<HashSet<&'static str>> = std::sync::OnceLock::new();
    NAMES.get_or_init(|| {
        [
            "sstable",
            "generation",
            "format",
            "position",
            "row_kind",
            "partition_deletion_time",
            "partition_deletion_timestamp",
            "bound_inclusive",
            "range_deletion_time",
            "range_deletion_timestamp",
        ]
        .into_iter()
        .collect()
    })
}

/// Split `predicates` into the subset that must apply to EVERY row
/// (partition-key / `token(...)` predicates, plus source-identity/
/// `row_kind`/partition-and-range-deletion predicates —
/// [`always_applicable_column_names`]) and the subset that applies ONLY to
/// a plain data row (`row_kind = 'row'`): clustering-key / regular-column
/// predicates and their metadata derivatives, plus the row-level metadata
/// quintet (roborev finding, issue #4222 — round 5, correcting round 2's
/// over-broad exemption).
///
/// Used by both row producers to apply the CORRECT predicate backstop per
/// row-kind: a synthetic `partition_tombstone`/`range_tombstone_*` row
/// carries no clustering/regular columns to test a data-only predicate
/// against, and the spec requires it to stay visible "even when the
/// generation holds no live rows" — but it DOES carry partition-key values
/// (`insert_pk_values`) and source/row_kind/deletion metadata, so those
/// predicates must still apply to it, or a full scan filtered to one
/// partition/generation would wrongly return every OTHER partition's or
/// generation's tombstone rows too.
pub(super) fn split_predicates_by_key_role<'a>(
    predicates: &'a [SSTablePredicate],
    base_schema: &TableSchema,
) -> (Vec<&'a SSTablePredicate>, Vec<&'a SSTablePredicate>) {
    let pk_names: HashSet<&str> = base_schema
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    let always_names = always_applicable_column_names();
    predicates.iter().partition(|p| {
        p.is_token()
            || pk_names.contains(p.column.as_str())
            || always_names.contains(p.column.as_str())
    })
}

/// Apply the raw view's post-scan predicate backstop to one row:
/// `always_predicates` (partition-key/token PLUS source-identity/
/// `row_kind`/partition-and-range-deletion predicates —
/// [`split_predicates_by_key_role`]) apply UNCONDITIONALLY, since every row
/// carries real values for those columns, synthetic or not. `data_predicates`
/// (clustering-key/regular-column predicates, their metadata derivatives,
/// and the row-level metadata quintet) apply ONLY to a plain data row
/// (`row_kind = 'row'`) — a synthetic row is exempt from those, since it
/// never carries them at all (roborev finding, issue #4222).
pub(super) fn row_passes_predicates(
    row: &QueryRow,
    always_predicates: &[&SSTablePredicate],
    data_predicates: &[&SSTablePredicate],
) -> Result<bool> {
    for p in always_predicates {
        if super::evaluate_leaf(row, p) != super::LeafOutcome::True {
            return Ok(false);
        }
    }
    if is_plain_data_row(row) {
        for p in data_predicates {
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
        // PER PARTITION LIMIT (roborev finding, issue #4222): silently
        // ignoring it (rather than failing closed, like the three guards
        // above) would return every row of every partition instead of
        // capping each one — the same silent-wrong-answer class.
        if plan.statement.per_partition_limit.is_some() {
            return Err(Error::unsupported_query(
                "PER PARTITION LIMIT is not supported over a _raw_sstable_data view",
            ));
        }
        // A WHERE clause with ANY comparison that did not fully lower to a
        // pushed-down predicate (roborev finding, issue #4222 — High,
        // round 2 of this same class): OR/NOT are ONE way that happens
        // (`collect_sstable_predicates` skips those branches entirely), but
        // `!=`/`NotIn`/`LIKE`/`IS NULL`/`IS NOT NULL`/a non-literal RHS
        // etc. ALSO silently drop even inside a perfectly pushable `AND`
        // position — `column_comparison_to_predicate` returns `None` for
        // every one of those shapes. The base pipeline compensates with a
        // residual `Filter` execution step over the ORIGINAL where-
        // expression tree — a step this view's early return never reaches.
        // Comparing the PUSHABLE leaf count against the predicate count
        // (rather than just checking for OR/NOT, or checking
        // `sstable_predicates.is_empty()`) catches BOTH classes: `pk = 1 AND
        // val != 'x'` still yields the non-empty list `[pk = 1]`, which an
        // emptiness check would miss. Failing closed here (rather than
        // re-implementing the general WHERE-expression evaluator with this
        // view's row-kind exemption semantics) is the same documented scope
        // boundary as ORDER BY/DISTINCT/aggregates above.
        if let Some(where_clause) = &plan.statement.where_clause {
            let fully_pushed = count_pushable_comparison_leaves(where_clause)
                .map(|n| n == plan.sstable_predicates.len())
                .unwrap_or(false);
            if !fully_pushed {
                return Err(Error::unsupported_query(
                    "a WHERE clause containing OR/NOT, or a comparison shape (!=, LIKE, IS \
                     [NOT] NULL, NOT IN, a non-literal comparison, etc.) that cannot be pushed \
                     down to an SSTable-level predicate, is not supported over a \
                     _raw_sstable_data view — every restriction must lower to a pushable \
                     column/token comparison, or it would be silently dropped",
                ));
            }
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
                    self.max_result_bytes,
                    self.max_result_rows,
                    stop_after,
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
                    self.max_result_bytes,
                    self.max_result_rows,
                    stop_after,
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
        // the two never drift. `SELECT *` is the only other shape handled
        // here; DISTINCT/aggregates are already rejected above, before any
        // producer work ran.
        //
        // Every OTHER `SelectExpression` shape (`WRITETIME`/`TTL`, an
        // alias, an arithmetic expression, a collection-element access) now
        // FAILS CLOSED (roborev finding, issue #4222 — round 5) rather than
        // silently falling through to "return every column unfiltered": the
        // prior wildcard `_ => (rows, columns)` arm matched BOTH
        // `SelectClause::All` (the correct behavior) AND a `Columns(exprs)`
        // whose list contained anything OTHER than a bare column reference
        // — so `SELECT WRITETIME(val) FROM t_raw_sstable_data` silently
        // returned every column instead of the WRITETIME projection (which
        // this view does not build) or a typed refusal, the same
        // silent-wrong-answer class ORDER BY/DISTINCT/aggregates are
        // rejected for above. Reshaping expressions over the raw view remain
        // OUT OF SCOPE for this slice (design.md D4's JOIN-gap precedent),
        // but the refusal must be EXPLICIT, never indistinguishable from
        // `SELECT *`.
        let (rows, columns) = match &plan.statement.select_clause {
            SelectClause::All => (rows, columns),
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
            SelectClause::Columns(_) | SelectClause::Distinct(_) => {
                return Err(Error::unsupported_query(
                    "a SELECT expression other than a plain column reference (WRITETIME/TTL, \
                     an alias, an arithmetic expression, or a collection-element access) is \
                     not supported over a _raw_sstable_data view — use 'SELECT *' or a plain \
                     column list",
                ));
            }
        };

        // Apply the SAME LIMIT/OFFSET the producer was told to `stop_after`
        // (mirroring the constant-query branch, `execute.rs`'s `SELECT 1`
        // handling): the producer stopped once `offset + limit` ACCEPTED
        // rows were collected (or ran out of corpus first), so this trims
        // exactly `offset` off the front and caps at `limit` — never fewer
        // than available, never more than requested.
        let rows: Vec<_> = rows.into_iter().skip(offset).take(limit).collect();

        // Same final budget check every other query path applies (issue
        // #1582/D6) — belt-and-braces alongside BOTH producers' own
        // incremental checks (issue #4222 roborev finding: the point-key
        // path now bounds accumulation the same way the full-scan path
        // does, via `PointCollector` in `point.rs`), never materializing
        // past the budget before this runs. Applied to the POST-limit rows,
        // so a `LIMIT 10` raw-view query is never penalized for the corpus
        // it did not keep.
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
        // `fully_qualified_match` is `false` for TWO distinct reasons
        // (`SSTableManager::fully_qualified_match`'s contract): a qualified
        // name that resolved via the bare-name fallback, OR nothing
        // resolving at all (`readers.is_empty()`). Only the FIRST is the
        // cross-keyspace safety violation this guard exists for (roborev
        // finding, issue #4222 — round 2): a schema-loaded-but-no-SSTables
        // table (a real, benign zero-row case) must fall through to an
        // empty result, not be misreported as a keyspace-mismatch error.
        if was_qualified && !fully_qualified_match && !readers.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaManager;
    use crate::storage::StorageEngine;
    use crate::{platform::Platform, Config};
    use tempfile::TempDir;

    async fn test_executor() -> (super::super::SelectExecutor, Arc<SchemaManager>) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            StorageEngine::open(
                temp_dir.path(),
                &config,
                platform.clone(),
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());
        (
            super::super::SelectExecutor::new(schema.clone(), storage),
            schema,
        )
    }

    /// Roborev finding (issue #4222): a REAL table literally named with the
    /// `_raw_sstable_data` suffix must resolve as an ordinary table, never be
    /// shadowed by the naming convention. Registering a schema for the exact
    /// literal name and asserting `raw_view_base_name` returns `None` for it
    /// is the direct regression test for that precedence.
    #[tokio::test]
    async fn literal_raw_sstable_data_table_is_never_shadowed() {
        let (executor, schema) = test_executor().await;
        schema
            .parse_and_register_cql_schema(
                "CREATE TABLE ks.foo_raw_sstable_data (pk int PRIMARY KEY, v text);",
            )
            .await
            .expect("registering the literal table's schema must succeed");

        let literal_id = TableId::new("ks.foo_raw_sstable_data");
        assert_eq!(
            executor.raw_view_base_name(&literal_id).await,
            None,
            "a REAL table literally named '..._raw_sstable_data' must resolve normally, \
             never be intercepted as the raw view's naming convention"
        );

        // Sanity: a table with NO literal registration under the suffixed
        // name IS recognized as the raw-view convention.
        schema
            .parse_and_register_cql_schema("CREATE TABLE ks.bar (pk int PRIMARY KEY, v text);")
            .await
            .expect("registering bar's schema must succeed");
        let convention_id = TableId::new("ks.bar_raw_sstable_data");
        assert_eq!(
            executor.raw_view_base_name(&convention_id).await,
            Some("bar".to_string()),
            "with no literal 'bar_raw_sstable_data' table registered, the suffix must be \
             recognized as the D1 naming convention over 'bar'"
        );
    }
}
