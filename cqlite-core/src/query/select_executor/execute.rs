//! Async pipeline entry points for the SELECT executor (issue #1174).
//!
//! This submodule continues the [`SelectExecutor`](super::SelectExecutor) `impl`
//! with the three large `async` pipeline methods that drive query execution:
//! - [`SelectExecutor::execute`] — the materializing plan runner,
//! - [`SelectExecutor::execute_streaming_background`] — the streaming producer
//!   task spawned by `execute_streaming` (which stays in `mod.rs`),
//! - [`SelectExecutor::execute_sstable_scan`] — the SSTable-scan step.
//!
//! These were relocated verbatim from `mod.rs` (epic #1116 file-size split); the
//! per-step helpers they call (`execute_filter`, `execute_sort`, etc.) and the
//! `ExecutionContext` bookkeeping struct remain in `mod.rs`. As a child module,
//! this file can reach `mod.rs`'s private items directly — the logic, ordering,
//! and error handling are unchanged.

use super::{
    build_row_from_scan, classify_partition_lookup, column_info_from_type_str, evaluate_predicates,
    honest_targeted_path, parse_table_id, project_expr_reshapes_row, select_has_writetime_ttl,
    sort_rows_by_token, validate_token_predicates, PartitionLookupOutcome, SSTablePredicate,
};
use super::{
    AccessPath, ColumnInfo, ExecutionContext, ExecutionStep, FallbackReason, OptimizedQueryPlan,
    ProjectionFlags, QueryResult, QueryRow, Result, SelectExecutor, TableId, TableSchema,
};
use crate::query::result_budget::enforce_materialized_rows;
use std::sync::Arc;

impl SelectExecutor {
    /// Execute an optimized query plan.
    ///
    /// Instrumented as `query.select.plan` (issue #1035): this span covers the
    /// modern SELECT pipeline — SSTable scan, filtering, projection, aggregation,
    /// and WRITETIME/TTL metadata extraction — and is the parent under which the
    /// read-path spans (issue #1034) nest. On completion it emits
    /// [`catalog::QUERY_ROWS_SCANNED`] (rows the scan step examined) dimensioned by
    /// the honest access path, so the rows-scanned vs rows-returned gap is
    /// observable. The bounded access-path attribute is recorded on the span; the
    /// query text and key values never are.
    #[tracing::instrument(
        name = "query.select.plan",
        skip_all,
        fields(
            cqlite.query.access_path = tracing::field::Empty,
            cqlite.query.rows_scanned = tracing::field::Empty,
            cqlite.query.rows = tracing::field::Empty,
        )
    )]
    pub async fn execute(&self, plan: OptimizedQueryPlan) -> Result<QueryResult> {
        // Issue #960: clear the global access-path probe so a stale value from a
        // previous query cannot satisfy a test assertion against this one.
        crate::query::access_path::reset();

        let table_id = if let Some(ref from_clause) = plan.statement.from_clause {
            self.extract_table_id(from_clause)?
        } else {
            // For queries without FROM clause (like SELECT 1), use a dummy table ID
            TableId::new("_dummy_")
        };

        // Issue #1587 (E5): resolve the table's schema ONCE per query into a shared
        // `Arc<TableSchema>`. Column-metadata building, the SSTable scan, and the
        // SELECT-* metadata fallback all borrow this same schema (ref-count bump),
        // instead of each independently re-locking the registry and deep-cloning a
        // fresh `TableSchema` (2–4 deep clones per query before this).
        let query_schema: Option<Arc<TableSchema>> = if plan.statement.from_clause.is_some() {
            self.resolve_table_schema(&table_id).await
        } else {
            None
        };

        // Issue #692: detect whether any WRITETIME/TTL select items are present
        // during planning and set the opt-in flag so the reader threads per-cell
        // metadata. This is the "planning" half of the executor wiring; the
        // "evaluation" half lives in `evaluate_select_expression`.
        let projection_flags = ProjectionFlags {
            include_cell_metadata: select_has_writetime_ttl(&plan.statement),
        };
        log::debug!(
            "Query plan: include_cell_metadata={}",
            projection_flags.include_cell_metadata
        );

        let mut context = ExecutionContext {
            table_id,
            columns: self.get_result_columns(&plan.statement, query_schema.as_deref())?,
            rows_processed: 0,
            scan_rows: 0,
            projection_flags,
            access_path: None,
            reverse_served: false,
        };

        // Handle queries without FROM clause (like SELECT 1)
        if plan.statement.from_clause.is_none() {
            let mut result = self.execute_constant_query(&plan.statement, &context)?;
            // Issue #1582 (roborev): apply the statement's LIMIT/OFFSET to the
            // constant rows BEFORE the byte + row-count budget check, so the budget
            // is enforced on the rows ACTUALLY returned (post LIMIT/OFFSET) —
            // consistent with the table-backed path below. In particular `LIMIT 0`
            // must return empty, never `ResultTooLarge`; an over-budget constant
            // SELECT with NO limit still trips the guard on its final rows.
            let offset = plan.statement.offset.unwrap_or(0) as usize;
            let limit = plan
                .statement
                .limit
                .as_ref()
                .map(|l| l.count as usize)
                .unwrap_or(usize::MAX);
            result.rows = result.rows.into_iter().skip(offset).take(limit).collect();
            // Keep the row-count metadata consistent with the returned rows.
            let returned = result.rows.len() as u64;
            result.rows_affected = returned;
            result.metadata.total_rows = Some(returned);
            enforce_materialized_rows(&result.rows, self.max_result_bytes, self.max_result_rows)?;
            return Ok(result);
        }

        // Execute the plan step by step
        let mut intermediate_results = Vec::new();

        // If no execution steps are provided, add a default table scan
        let execution_steps = if plan.execution_steps.is_empty() {
            vec![ExecutionStep::SSTableScan {
                table: context.table_id.clone(),
                predicates: vec![],
                projection: context.columns.iter().map(|c| c.name.clone()).collect(),
            }]
        } else {
            plan.execution_steps.clone()
        };

        // Issue #1578 (D2): fold a GROUP-BY-free aggregate over a full table scan
        // into an O(1) accumulator instead of buffering the whole table here.
        // Returns None for any plan shape it does not model (GROUP BY, targeted
        // lookup, WRITETIME/TTL, or a Sort/Project/Limit step) → the buffered step
        // loop below runs unchanged.
        if let Some(rows) = self
            .try_execute_global_aggregate(&execution_steps, query_schema.as_deref(), &mut context)
            .await?
        {
            intermediate_results = rows;
        } else {
            for step in &execution_steps {
                match step {
                    ExecutionStep::SSTableScan {
                        table,
                        predicates,
                        projection,
                        ..
                    } => {
                        let rows = self
                            .execute_sstable_scan(
                                table,
                                predicates,
                                projection,
                                plan.statement.order_by.as_ref(),
                                query_schema.as_deref(),
                                &mut context,
                            )
                            .await?;
                        intermediate_results = rows;
                    }
                    ExecutionStep::Filter { expression, .. } => {
                        intermediate_results =
                            self.execute_filter(intermediate_results, expression, &mut context)?;
                    }
                    ExecutionStep::Sort { order_by, .. } => {
                        // Issue #1184: when the BIG reverse partition iterator already
                        // produced the rows in descending clustering order, skip the
                        // in-memory sort entirely (it remains the fallback otherwise).
                        if !context.reverse_served {
                            intermediate_results =
                                self.execute_sort(intermediate_results, order_by, &mut context)?;
                        } else {
                            // Issue #1307 (hardening): skipping this Sort is sound ONLY
                            // because `reverse_served` is set exclusively by
                            // `targeted_partition_rows`, which serves the reverse
                            // promoted-index iterator precisely when `statement.order_by`
                            // requests the reverse of the stored clustering order — and
                            // the planner emits EXACTLY ONE `Sort` step, cloned from that
                            // same `statement.order_by` (see `select_optimizer.rs`). So
                            // the reverse scan's ordering matches this step's `order_by`,
                            // and skipping it drops a redundant sort rather than a
                            // different ordering. What would break the invariant: a
                            // multi-table / join plan (or any plan) that reused the flag
                            // across a Sort NOT derived from the reverse-served scan's
                            // `statement.order_by` — such a plan must clear
                            // `reverse_served` before this step. The debug_assert pins
                            // the property (the skipped Sort's key equals the statement's
                            // order_by); it is debug-only and never alters release
                            // behavior.
                            debug_assert!(
                                plan.statement.order_by.as_ref() == Some(order_by),
                                "reverse_served Sort-skip invariant violated: the skipped \
                             Sort's order_by must be the statement's order_by that drove \
                             the reverse-served scan (single-table plan); a plan whose \
                             Sort is not the reverse scan's matching Sort must clear \
                             reverse_served first",
                            );
                        }
                    }
                    ExecutionStep::Aggregate { plan: agg_plan, .. } => {
                        intermediate_results =
                            self.execute_aggregation(intermediate_results, agg_plan, &mut context)?;
                    }
                    ExecutionStep::PerPartitionLimit { count } => {
                        intermediate_results =
                            Self::execute_per_partition_limit(intermediate_results, *count);
                    }
                    ExecutionStep::Limit { count, offset } => {
                        intermediate_results = self.execute_limit(
                            intermediate_results,
                            *count,
                            *offset,
                            &mut context,
                        )?;
                    }
                    ExecutionStep::Project { columns } => {
                        // Issue #1952 (round-6 fix): branch on whether the projection
                        // RESHAPES the row. A plain-column Project only trims the
                        // #1952-widened helper columns — route it through the
                        // key-preserving `trim_projection` so the row keeps its real
                        // RowKey / metadata and a sparse row's absent selected cell is
                        // omitted rather than erroring. Only a reshaping / computed
                        // projection (alias, arithmetic, aggregate, function,
                        // writetime/ttl, collection-access) goes through
                        // `execute_projection`, whose empty-RowKey + name-derivation is
                        // correct for a computed row that has no natural stored key.
                        intermediate_results = if columns.iter().any(project_expr_reshapes_row) {
                            self.execute_projection(intermediate_results, columns, &mut context)?
                        } else {
                            self.trim_projection(intermediate_results, columns)
                        };
                    }
                }
            }
        }

        // Issue #1582 (D6, narrow subset): enforce the byte-bounded result budget
        // (primary) + the row-count safety valve (secondary) with a SINGLE robust
        // check on the FINAL materialized result — AFTER every execution step
        // (Limit/Offset/Filter/Sort/Aggregate/Project) has produced the rows that
        // will actually be returned. Because this sees ONLY the returned rows
        // (post LIMIT/OFFSET), a `LIMIT 10` query never trips and OFFSET-skipped
        // rows are never charged. This replaces the earlier during-collection
        // machinery (LIMIT/OFFSET pushdown, per-row early-stop, storage-layer row limit),
        // which kept generating correctness edge cases; a single final-result
        // check has no such edges. Reuses the shared `estimate_value_size`
        // estimator (via `enforce_materialized_rows`).
        //
        // SCOPE (owner-accepted boundaries, tracked on #1582 — NOT bugs to fix in
        // this narrow subset):
        //   * Does NOT bound PEAK scan memory: `storage.scan` still materializes
        //     each reader's matching rows before this point (deferred to #1897).
        //   * Does NOT cover the LEGACY point-lookup `QueryExecutor` path
        //     (`WHERE id = ?` short lookups route there, not through this modern
        //     executor) — deferred to the D6 redesign.
        // Issue #1578 (D2): demote the row-count valve to a genuine safety valve.
        // A query with an EXPLICIT `LIMIT` already bounds its own result, so it is
        // exempt from the crude row-count ceiling (the user accepted the count);
        // the byte budget still guards memory. Without an explicit LIMIT the valve
        // remains a real net against unbounded materialization.
        let effective_max_rows = if plan.statement.limit.is_some() {
            usize::MAX
        } else {
            self.max_result_rows
        };
        enforce_materialized_rows(
            &intermediate_results,
            self.max_result_bytes,
            effective_max_rows,
        )?;

        let total_rows = intermediate_results.len() as u64;

        // CRITICAL FIX (Issue #129/#140): Populate metadata.columns for SELECT *
        // When SELECT * is used and no schema was found, context.columns is empty.
        // Fall back to inferring column names from the first row's HashMap keys.
        // IMPORTANT: Must be sorted alphabetically for deterministic JSON output (Issue #129)!
        let mut columns = context.columns;
        if columns.is_empty() && !intermediate_results.is_empty() {
            // Issue #1587 (E5): reuse the schema resolved once at the top of the
            // query rather than re-locking the registry + deep-cloning here.
            let schema_opt = query_schema.as_deref();

            let first_row = &intermediate_results[0];
            let mut col_names: Vec<_> = first_row.values.keys().collect();
            col_names.sort(); // Sort alphabetically for deterministic ordering (Issue #129)

            let table_name_for_meta = schema_opt.map(|s| format!("{}.{}", s.keyspace, s.table));

            for (idx, col_name) in col_names.iter().enumerate() {
                let col_name: &str = col_name;
                // Look up CQL type from schema; derive flat DataType from it (Issue #674).
                let col_info = match schema_opt
                    .and_then(|schema| schema.columns.iter().find(|c| c.name.as_str() == col_name))
                {
                    Some(schema_col) => column_info_from_type_str(
                        col_name.to_string(),
                        &schema_col.data_type,
                        idx,
                        table_name_for_meta.clone(),
                    ),
                    None => ColumnInfo {
                        name: col_name.to_string(),
                        data_type: crate::types::DataType::Text,
                        nullable: true,
                        position: idx,
                        table_name: table_name_for_meta.clone(),
                        cql_type: None,
                    },
                };
                columns.push(col_info);
            }
        }

        // Observability (issue #1035): the `query.select.plan` span declared
        // `access_path`/`rows_scanned`/`rows` but never recorded them, and
        // `QUERY_ROWS_SCANNED` was never emitted. Do both here, sourced from the
        // honest per-query signal (`context.access_path`, set by the SSTable-scan
        // step) and the rows the scan examined (`context.rows_processed`). Bounded
        // attributes only — never the query text or key values.
        {
            use crate::observability::{self as obs, catalog, AttrValue};

            let access_path_label: &'static str = context
                .access_path
                .as_ref()
                .map(|p| p.label())
                .unwrap_or("unknown");

            obs::add_counter(
                catalog::QUERY_ROWS_SCANNED,
                context.scan_rows,
                &[(
                    catalog::attr::ACCESS_PATH,
                    AttrValue::StaticStr(access_path_label),
                )],
            );

            let span = tracing::Span::current();
            span.record(catalog::attr::ACCESS_PATH, access_path_label);
            span.record("cqlite.query.rows_scanned", context.scan_rows);
            span.record("cqlite.query.rows", total_rows);
        }

        // Issue #1035: carry a bounded plan family on the result so the engine's
        // single observability chokepoint reports a real plan type for SELECTs
        // (the modern executor previously always returned `plan_info: None`,
        // forcing plan_type to "unknown").
        let plan_info = Self::select_plan_info(&plan, context.access_path.as_ref());

        Ok(QueryResult {
            rows: intermediate_results,
            rows_affected: total_rows, // Use actual number of rows returned
            execution_time_ms: 0,      // Will be set by the engine
            metadata: crate::query::result::QueryMetadata {
                columns,
                total_rows: Some(total_rows),
                plan_info: Some(plan_info),
                performance: Default::default(),
                warnings: vec![],
                // Issue #960: surface the access path the SSTable-scan step chose
                // on the result from PER-QUERY state (not the global probe), so a
                // concurrent SELECT cannot overwrite it between record() and here.
                access_path: context.access_path.clone(),
            },
        })
    }

    /// Execute SSTable scan with predicate pushdown.
    ///
    /// Per-row work (build row, decode partition key, evaluate predicates) is
    /// handled by the free helpers `build_row_from_scan` and
    /// `evaluate_predicates`, which are shared with the streaming background
    /// task to keep the two execution paths in lockstep.
    ///
    /// Issue #1582 (D6, narrow subset): this scan NO LONGER applies any
    /// byte/row budget or LIMIT/OFFSET pushdown itself. It materializes the
    /// predicate-matching rows and returns them; the single byte/row budget check
    /// is applied ONCE by [`SelectExecutor::execute`] on the FINAL result, after
    /// the whole step pipeline. `storage.scan` receives no query-derived row limit
    /// (its `limit` argument stays `None`), so a `WHERE non_pk = ? LIMIT N` cannot
    /// silently drop matching rows past the first N raw rows.
    #[cfg_attr(feature = "tombstones", allow(unused_variables))]
    pub(super) async fn execute_sstable_scan(
        &self,
        table: &TableId,
        predicates: &[SSTablePredicate],
        projection: &[String],
        order_by: Option<&crate::query::select_ast::OrderByClause>,
        // Issue #1587 (E5): schema resolved ONCE per query by the caller and
        // shared by reference — no per-scan registry lock + deep clone.
        schema_opt: Option<&TableSchema>,
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        // FINDING 2 (Issue #955 follow-up): a `token(...)` predicate is evaluated
        // by hashing the row's raw partition key, so its argument columns MUST be
        // the full partition key in declared order or the result is silently
        // wrong. Reject (Cassandra-style) before scanning/evaluating.
        validate_token_predicates(predicates, schema_opt)?;

        // Data-safety (issue #1694): log the SHAPE of the scan — predicate count
        // and the constrained column names — never the predicate literals/values.
        log::debug!(
            "Executing SSTableScan: table=\"{}\", predicates={} on [{}], include_cell_metadata={}",
            table,
            predicates.len(),
            predicates
                .iter()
                .map(|p| p.column.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            context.projection_flags.include_cell_metadata,
        );

        let (keyspace, table_name) = parse_table_id(table);

        match schema_opt {
            Some(schema) => log::info!(
                "Found schema for {}.{} with {} columns",
                schema.keyspace,
                schema.table,
                schema.columns.len()
            ),
            None => log::info!(
                "No schema found for {}.{}, proceeding without schema-aware parsing",
                keyspace.as_deref().unwrap_or("unknown"),
                table_name
            ),
        }

        // Issue #693: When WRITETIME(col) or TTL(col) is in the SELECT, use the
        // metadata-carrying scan so per-cell timestamps reach the QueryRow.
        let mut results = Vec::new();
        if context.projection_flags.include_cell_metadata {
            // Issue #962: route a fully-constrained `WHERE pk = ?` WRITETIME/TTL
            // projection through a partition-targeted metadata lookup that prunes
            // SSTables (bloom/BTI) before decoding, instead of full-scanning every
            // SSTable for the table. Reuses the SAME `classify_partition_lookup`
            // decision the non-metadata path uses (the shared resolved
            // partition-lookup representation). The per-row predicate evaluation
            // below is unchanged, so the pk equality itself is still applied as a
            // correctness backstop and any bloom/BTI over-inclusion is filtered out.
            let scan_results = match classify_partition_lookup(predicates, schema_opt) {
                PartitionLookupOutcome::Targeted(pk_bytes) => {
                    log::info!(
                        "SSTableScan(metadata): partition-key point lookup (key len={}) for \"{}\"",
                        pk_bytes.len(),
                        table
                    );
                    // Epic #951 (honest paths): the `tombstones` build's metadata
                    // lookup is a full metadata scan + retain with NO prune,
                    // reported via `engaged == false`; claim
                    // `MetadataPartitionLookup` only when it really pruned, else
                    // report the honest `TombstonesBuildNoPrune` fallback (the
                    // rows are byte-identical either way).
                    let (rows, engaged) = self
                        .storage
                        .scan_partition_with_cell_metadata(table, &pk_bytes, schema_opt)
                        .await?;
                    let path = honest_targeted_path(AccessPath::MetadataPartitionLookup, engaged);
                    context.access_path = Some(path.clone());
                    crate::query::access_path::record(path);
                    rows
                }
                // Issue #962: `WHERE pk IN (...)` on the metadata path is NOT yet
                // fanned out to N targeted metadata lookups; it still full-scans.
                // Report that honestly (MetadataScanPath) rather than faking a
                // targeted path — the IN-metadata fan-out is a documented follow-up.
                PartitionLookupOutcome::MultiTargeted(_) | PartitionLookupOutcome::Fallback(_) => {
                    let metadata_path = AccessPath::FallbackFullScan {
                        reason: FallbackReason::MetadataScanPath,
                    };
                    context.access_path = Some(metadata_path.clone());
                    crate::query::access_path::record(metadata_path);
                    self.storage
                        .scan_with_cell_metadata(table, None, None, None, schema_opt)
                        .await?
                }
            };

            log::info!("Scan (with metadata) returned {} rows", scan_results.len());

            for (key, value, cell_meta) in scan_results {
                context.rows_processed += 1;
                context.scan_rows += 1;

                let Some(mut row) = build_row_from_scan(key, value, projection, schema_opt) else {
                    continue;
                };

                // Attach per-cell metadata so evaluate_writetime_ttl can read it.
                if !cell_meta.is_empty() {
                    row.set_cell_metadata(cell_meta);
                }

                if evaluate_predicates(&row, predicates)? {
                    results.push(row);
                }
            }
        } else {
            // Issue #949: a fully-constrained `WHERE pk = ?` is served by a
            // partition-targeted lookup that prunes SSTables via bloom/BTI and only
            // parses the candidates, instead of scanning every SSTable for the
            // table. Falls back to a full scan when the partition key isn't fully
            // pinned or can't be encoded. The per-row predicate evaluation below is
            // unchanged, so clustering predicates and the pk equality itself are
            // still applied (and any over-inclusion is filtered out).
            let scan_results = match classify_partition_lookup(predicates, schema_opt) {
                PartitionLookupOutcome::Targeted(pk_bytes) => {
                    log::info!(
                        "SSTableScan: partition-key point lookup (key len={}) for \"{}\"",
                        pk_bytes.len(),
                        table
                    );
                    // Issue #954: when a single-column clustering restriction is
                    // present, push it down to a within-partition seek so a wide
                    // partition's slice decodes O(matched rows + index), not the
                    // whole partition. The seek reports whether the clustering
                    // narrowing actually engaged; the per-row backstop below applies
                    // the exact bound so output is byte-identical either way.
                    //
                    // Issue #960: report the HONEST access path — `ClusteringSlice`
                    // only when the seek engaged, else `PartitionLookup`. The
                    // clustering seek exists only on the default build; the
                    // `tombstones` build uses the plain partition lookup.
                    // Issue #954/#960/#1184: forward clustering-slice seek OR (for
                    // `ORDER BY <ck>` reverse-of-stored) the BIG reverse iterator,
                    // with the honest access path recorded inside the helper.
                    #[cfg(not(feature = "tombstones"))]
                    {
                        self.targeted_partition_rows(
                            table, &pk_bytes, predicates, order_by, schema_opt, context,
                        )
                        .await?
                    }
                    #[cfg(feature = "tombstones")]
                    {
                        // Epic #951 (honest paths): the `tombstones` build's
                        // `scan_partition` is a full scan + retain with NO prune,
                        // reported via `engaged == false`. Report the honest
                        // fallback rather than a fake `PartitionLookup`; the rows
                        // are byte-identical to the pruned build.
                        let (rows, engaged) = self
                            .storage
                            .scan_partition(table, &pk_bytes, schema_opt)
                            .await?;
                        let path = honest_targeted_path(AccessPath::PartitionLookup, engaged);
                        context.access_path = Some(path.clone());
                        crate::query::access_path::record(path);
                        rows
                    }
                }
                PartitionLookupOutcome::MultiTargeted(pk_keys) => {
                    log::info!(
                        "SSTableScan: multi-partition lookup ({} keys) for \"{}\"",
                        pk_keys.len(),
                        table
                    );
                    // Issue #955/#960: `WHERE pk IN (...)` over the complete key
                    // is the union of N independent partition-targeted lookups,
                    // each of which prunes SSTables. Epic #951 (honest paths): on
                    // the `tombstones` build each lookup full-scans + retains with
                    // NO prune (`engaged == false`); report `MultiPartitionLookup`
                    // only when the lookups actually pruned, else the honest
                    // `TombstonesBuildNoPrune` fallback. Rows are unchanged.
                    let mut combined = Vec::new();
                    let mut all_engaged = true;
                    for pk_bytes in &pk_keys {
                        let (rows, engaged) = self
                            .storage
                            .scan_partition(table, pk_bytes, schema_opt)
                            .await?;
                        all_engaged &= engaged;
                        combined.extend(rows);
                    }
                    let path = honest_targeted_path(AccessPath::MultiPartitionLookup, all_engaged);
                    context.access_path = Some(path.clone());
                    crate::query::access_path::record(path);
                    // Order the union to equal a full scan filtered to these keys:
                    // partitions are stored token-ordered, so sort the combined
                    // rows by (partition token, raw key bytes). A *stable* sort
                    // keeps each partition's clustering order (rows for one key
                    // arrive contiguously from one `scan_partition`) intact.
                    sort_rows_by_token(&mut combined);
                    combined
                }
                PartitionLookupOutcome::Fallback(reason) => {
                    // Issue #960: report the honest reason a full scan was chosen.
                    context.access_path = Some(AccessPath::FallbackFullScan { reason });
                    crate::query::access_path::record(AccessPath::FallbackFullScan { reason });
                    // Issue #1582 (D6, narrow subset): no query-derived row limit
                    // is pushed into the predicate-unaware `storage.scan` — the sole
                    // budget check is applied once on the FINAL result in `execute`.
                    self.storage
                        .scan(table, None, None, None, schema_opt)
                        .await?
                }
            };

            log::info!("Scan returned {} rows", scan_results.len());

            for (key, value) in scan_results {
                context.rows_processed += 1;
                context.scan_rows += 1;

                // build_row_from_scan returns None for tombstoned/null rows (Issue #191).
                let Some(row) = build_row_from_scan(key, value, projection, schema_opt) else {
                    continue;
                };

                if evaluate_predicates(&row, predicates)? {
                    results.push(row);
                }
            }
        }

        Ok(results)
    }
}
