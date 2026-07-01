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
    honest_targeted_path, parse_table_id, select_has_writetime_ttl, sort_rows_by_token,
    validate_token_predicates, PartitionLookupOutcome, SSTablePredicate,
};
use super::{
    AccessPath, ColumnInfo, Error, ExecutionContext, ExecutionStep, FallbackReason,
    OptimizedQueryPlan, ProjectionFlags, QueryResult, QueryRow, Result, SchemaManager,
    SelectExecutor, StorageEngine, TableId,
};
use std::sync::Arc;
use tokio::sync::mpsc;

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
            columns: self.get_result_columns(&plan.statement).await?,
            rows_processed: 0,
            scan_rows: 0,
            projection_flags,
            access_path: None,
            reverse_served: false,
        };

        // Handle queries without FROM clause (like SELECT 1)
        if plan.statement.from_clause.is_none() {
            return self.execute_constant_query(&plan.statement, &context).await;
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
                            &mut context,
                        )
                        .await?;
                    intermediate_results = rows;
                }
                ExecutionStep::Filter { expression, .. } => {
                    intermediate_results = self
                        .execute_filter(intermediate_results, expression, &mut context)
                        .await?;
                }
                ExecutionStep::Sort { order_by, .. } => {
                    // Issue #1184: when the BIG reverse partition iterator already
                    // produced the rows in descending clustering order, skip the
                    // in-memory sort entirely (it remains the fallback otherwise).
                    if !context.reverse_served {
                        intermediate_results = self
                            .execute_sort(intermediate_results, order_by, &mut context)
                            .await?;
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
                    intermediate_results = self
                        .execute_aggregation(intermediate_results, agg_plan, &mut context)
                        .await?;
                }
                ExecutionStep::PerPartitionLimit { count } => {
                    intermediate_results =
                        Self::execute_per_partition_limit(intermediate_results, *count);
                }
                ExecutionStep::Limit { count, offset } => {
                    intermediate_results = self
                        .execute_limit(intermediate_results, *count, *offset, &mut context)
                        .await?;
                }
                ExecutionStep::Project { columns } => {
                    intermediate_results = self
                        .execute_projection(intermediate_results, columns, &mut context)
                        .await?;
                }
            }
        }

        let total_rows = intermediate_results.len() as u64;

        // CRITICAL FIX (Issue #129/#140): Populate metadata.columns for SELECT *
        // When SELECT * is used and no schema was found, context.columns is empty.
        // Fall back to inferring column names from the first row's HashMap keys.
        // IMPORTANT: Must be sorted alphabetically for deterministic JSON output (Issue #129)!
        let mut columns = context.columns;
        if columns.is_empty() && !intermediate_results.is_empty() {
            // Try to resolve schema to get proper CQL types (Issue #674).
            let schema_opt = if let Some(ref from_clause) = plan.statement.from_clause {
                if let Ok(table_id) = self.extract_table_id(from_clause) {
                    let (keyspace, table_name) = parse_table_id(&table_id);
                    self._schema
                        .find_schema_by_table(&keyspace, &table_name)
                        .await
                } else {
                    None
                }
            } else {
                None
            };

            let first_row = &intermediate_results[0];
            let mut col_names: Vec<_> = first_row.values.keys().collect();
            col_names.sort(); // Sort alphabetically for deterministic ordering (Issue #129)

            let table_name_for_meta = schema_opt
                .as_ref()
                .map(|s| format!("{}.{}", s.keyspace, s.table));

            for (idx, col_name) in col_names.iter().enumerate() {
                let col_name: &str = col_name;
                // Look up CQL type from schema; derive flat DataType from it (Issue #674).
                let col_info = match schema_opt
                    .as_ref()
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

    /// Background task: Execute streaming scan and send rows through channel
    pub(super) async fn execute_streaming_background(
        storage: Arc<StorageEngine>,
        schema_manager: Arc<SchemaManager>,
        _table_id: TableId,
        execution_steps: Vec<ExecutionStep>,
        tx: mpsc::Sender<Result<QueryRow>>,
        buffer_size: usize,
    ) -> Result<()> {
        // Issue #581: LIMIT/OFFSET must be enforced by the producer in the
        // streaming path. The `ExecutionStep::Limit` arm previously only logged a
        // message and relied on a consumer that never applied it, so
        // `execute_streaming` yielded the full result set regardless of LIMIT.
        // Extract the bound up front (steps are ordered with Limit after the scan)
        // and stop sending once it is satisfied — mirroring `execute_limit`
        // (drain OFFSET, then truncate to `count`) row-by-row so the producer
        // stops scanning early.
        let limit = execution_steps.iter().find_map(|step| match step {
            ExecutionStep::Limit { count, offset } => Some((*count, offset.unwrap_or(0))),
            _ => None,
        });
        let (limit_count, mut offset_remaining) = match limit {
            Some((count, offset)) => (Some(count), offset),
            None => (None, 0),
        };

        // A `LIMIT 0` means no rows can ever be sent; return before scanning.
        if limit_count == Some(0) {
            return Ok(());
        }

        // Issue #757: PER PARTITION LIMIT caps rows per partition before the
        // query-wide LIMIT/OFFSET. The scan yields rows grouped by partition
        // key, so we track the current partition (by its raw key bytes) and
        // reset the counter at each boundary.
        let per_partition_limit = execution_steps.iter().find_map(|step| match step {
            ExecutionStep::PerPartitionLimit { count } => Some(*count),
            _ => None,
        });
        let mut current_partition: Option<Vec<u8>> = None;
        let mut partition_count: u64 = 0;

        let mut sent: u64 = 0;

        for step in &execution_steps {
            match step {
                ExecutionStep::SSTableScan {
                    table,
                    predicates,
                    projection,
                    ..
                } => {
                    let (keyspace, table_name) = parse_table_id(table);
                    let schema_opt = schema_manager
                        .find_schema_by_table(&keyspace, &table_name)
                        .await;

                    // FINDING 2 (Issue #955 follow-up): reject a `token(...)` whose
                    // columns are not the full partition key in declared order
                    // before scanning (same rule as the materializing path).
                    validate_token_predicates(predicates, schema_opt.as_ref())?;

                    // Issue #949: a fully-constrained `WHERE pk = ?` is served by a
                    // partition-targeted lookup that prunes SSTables via bloom/BTI,
                    // instead of streaming a scan over every SSTable. The resulting
                    // rows are sent through the same per-row pipeline below
                    // (predicates, PER PARTITION LIMIT, OFFSET, LIMIT). Note
                    // `scan_partition` reconciles across SSTable generations like the
                    // materializing `scan()` (last-write-wins + tombstone shadowing),
                    // which is the authoritative read semantics; it does not merely
                    // mirror `scan_stream`'s per-key merge.
                    let lookup = classify_partition_lookup(predicates, schema_opt.as_ref());
                    if let PartitionLookupOutcome::Targeted(ref pk_bytes) = lookup {
                        // Issue #960: the streaming analogue of the materializing
                        // partition-targeted lookup. Epic #951 (honest paths): the
                        // `tombstones` build's `scan_partition` is a full-scan +
                        // retain with NO prune, reported via `engaged == false`; only
                        // claim `StreamingPartitionLookup` when it really pruned.
                        let (rows, engaged) = storage
                            .scan_partition(table, pk_bytes, schema_opt.as_ref())
                            .await?;
                        crate::query::access_path::record(honest_targeted_path(
                            AccessPath::StreamingPartitionLookup,
                            engaged,
                        ));
                        for (key, value) in rows {
                            let part_sig = per_partition_limit.map(|_| key.0.clone());
                            let Some(row) =
                                build_row_from_scan(key, value, projection, schema_opt.as_ref())
                            else {
                                continue;
                            };
                            if !evaluate_predicates(&row, predicates)? {
                                continue;
                            }
                            if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                                if current_partition.as_deref() != Some(sig.as_slice()) {
                                    current_partition = Some(sig);
                                    partition_count = 0;
                                }
                                if partition_count >= cap {
                                    continue;
                                }
                                partition_count += 1;
                            }
                            if offset_remaining > 0 {
                                offset_remaining -= 1;
                                continue;
                            }
                            if tx.send(Ok(row)).await.is_err() {
                                return Ok(());
                            }
                            sent += 1;
                            if let Some(count) = limit_count {
                                if sent >= count {
                                    return Ok(());
                                }
                            }
                        }
                        // This SSTableScan step is fully served by the lookup.
                        continue;
                    }

                    // Issue #955: `WHERE pk IN (...)` over the complete key is the
                    // union of N partition-targeted lookups. Gather them, sort by
                    // token to match full-scan order, then drive the same per-row
                    // pipeline (predicates, PER PARTITION LIMIT, OFFSET, LIMIT).
                    if let PartitionLookupOutcome::MultiTargeted(ref pk_keys) = lookup {
                        // Epic #951 (honest paths): each lookup reports whether it
                        // pruned. On the `tombstones` build every call full-scans
                        // (`engaged == false`); claim `MultiPartitionLookup` only when
                        // the lookups actually pruned, else report the honest fallback.
                        let mut combined = Vec::new();
                        let mut all_engaged = true;
                        for pk_bytes in pk_keys {
                            let (rows, engaged) = storage
                                .scan_partition(table, pk_bytes, schema_opt.as_ref())
                                .await?;
                            all_engaged &= engaged;
                            combined.extend(rows);
                        }
                        crate::query::access_path::record(honest_targeted_path(
                            AccessPath::MultiPartitionLookup,
                            all_engaged,
                        ));
                        sort_rows_by_token(&mut combined);
                        for (key, value) in combined {
                            let part_sig = per_partition_limit.map(|_| key.0.clone());
                            let Some(row) =
                                build_row_from_scan(key, value, projection, schema_opt.as_ref())
                            else {
                                continue;
                            };
                            if !evaluate_predicates(&row, predicates)? {
                                continue;
                            }
                            if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                                if current_partition.as_deref() != Some(sig.as_slice()) {
                                    current_partition = Some(sig);
                                    partition_count = 0;
                                }
                                if partition_count >= cap {
                                    continue;
                                }
                                partition_count += 1;
                            }
                            if offset_remaining > 0 {
                                offset_remaining -= 1;
                                continue;
                            }
                            if tx.send(Ok(row)).await.is_err() {
                                return Ok(());
                            }
                            sent += 1;
                            if let Some(count) = limit_count {
                                if sent >= count {
                                    return Ok(());
                                }
                            }
                        }
                        // This SSTableScan step is fully served by the lookups.
                        continue;
                    }

                    // Issue #960: the streaming path did not take a targeted
                    // lookup; report the honest fallback reason. `lookup` is the
                    // `Fallback` arm here (the `Targeted`/`MultiTargeted` arms
                    // returned above via `continue`).
                    if let PartitionLookupOutcome::Fallback(reason) = lookup {
                        crate::query::access_path::record(AccessPath::FallbackFullScan { reason });
                    }

                    // Issue #790: pull rows lazily from a bounded streaming scan
                    // instead of materializing the full result `Vec`. The reader
                    // parses one entry at a time into this channel, so live heap
                    // stays bounded by `buffer_size` rather than O(result rows).
                    let mut scan_stream = storage
                        .scan_stream(table, None, None, schema_opt.as_ref(), buffer_size)
                        .await?;

                    while let Some(item) = scan_stream.recv().await {
                        let (key, value) = item?;
                        // Capture the partition key bytes before `key` is moved
                        // into row construction (only when needed).
                        let part_sig = per_partition_limit.map(|_| key.0.clone());
                        let Some(row) =
                            build_row_from_scan(key, value, projection, schema_opt.as_ref())
                        else {
                            continue;
                        };

                        if !evaluate_predicates(&row, predicates)? {
                            continue;
                        }

                        // Apply PER PARTITION LIMIT: cap matching rows per
                        // partition, before OFFSET/LIMIT (Cassandra semantics).
                        if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                            if current_partition.as_deref() != Some(sig.as_slice()) {
                                current_partition = Some(sig);
                                partition_count = 0;
                            }
                            if partition_count >= cap {
                                continue;
                            }
                            partition_count += 1;
                        }

                        // Apply OFFSET: skip the first `offset_remaining` matches.
                        if offset_remaining > 0 {
                            offset_remaining -= 1;
                            continue;
                        }

                        // Send row through channel (with backpressure). Consumer drop ends the scan.
                        if tx.send(Ok(row)).await.is_err() {
                            return Ok(());
                        }
                        sent += 1;

                        // Apply LIMIT: stop scanning once `count` rows have been
                        // sent. Dropping `scan_stream` here signals the producer
                        // (via a closed channel) to stop parsing early.
                        if let Some(count) = limit_count {
                            if sent >= count {
                                return Ok(());
                            }
                        }
                    }
                }
                ExecutionStep::Limit { .. } | ExecutionStep::PerPartitionLimit { .. } => {
                    // Enforced inline during the scan above (see the bounds
                    // extracted before the loop).
                }
                // Projection and predicate filtering are pushed into SSTableScan above.
                ExecutionStep::Project { .. } | ExecutionStep::Filter { .. } => {}
                _ => {
                    log::warn!("Streaming execution: skipping unsupported step {:?}", step);
                }
            }
        }

        Ok(())
    }

    /// Execute SSTable scan with predicate pushdown.
    ///
    /// Per-row work (build row, decode partition key, evaluate predicates) is
    /// handled by the free helpers `build_row_from_scan` and
    /// `evaluate_predicates`, which are shared with the streaming background
    /// task to keep the two execution paths in lockstep.
    #[cfg_attr(feature = "tombstones", allow(unused_variables))]
    pub(super) async fn execute_sstable_scan(
        &self,
        table: &TableId,
        predicates: &[SSTablePredicate],
        projection: &[String],
        order_by: Option<&crate::query::select_ast::OrderByClause>,
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        const MAX_RESULTS: usize = 1_000_000;

        log::info!(
            "Executing SSTableScan: table=\"{}\", predicates={:?}, include_cell_metadata={}",
            table,
            predicates,
            context.projection_flags.include_cell_metadata,
        );

        let (keyspace, table_name) = parse_table_id(table);
        let schema_opt = self
            ._schema
            .find_schema_by_table(&keyspace, &table_name)
            .await;

        match schema_opt.as_ref() {
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

        // FINDING 2 (Issue #955 follow-up): a `token(...)` predicate is evaluated
        // by hashing the row's raw partition key, so its argument columns MUST be
        // the full partition key in declared order or the result is silently
        // wrong. Reject (Cassandra-style) before scanning/evaluating.
        validate_token_predicates(predicates, schema_opt.as_ref())?;

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
            let scan_results = match classify_partition_lookup(predicates, schema_opt.as_ref()) {
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
                        .scan_partition_with_cell_metadata(table, &pk_bytes, schema_opt.as_ref())
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
                        .scan_with_cell_metadata(table, None, None, None, schema_opt.as_ref())
                        .await?
                }
            };

            log::info!("Scan (with metadata) returned {} rows", scan_results.len());

            for (key, value, cell_meta) in scan_results {
                context.rows_processed += 1;
                context.scan_rows += 1;

                let Some(mut row) =
                    build_row_from_scan(key, value, projection, schema_opt.as_ref())
                else {
                    continue;
                };

                // Attach per-cell metadata so evaluate_writetime_ttl can read it.
                if !cell_meta.is_empty() {
                    row.set_cell_metadata(cell_meta);
                }

                if evaluate_predicates(&row, predicates)? {
                    results.push(row);
                }

                if results.len() > MAX_RESULTS {
                    return Err(Error::query_execution(
                        "Result set too large, consider adding LIMIT".to_string(),
                    ));
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
            let scan_results = match classify_partition_lookup(predicates, schema_opt.as_ref()) {
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
                            table,
                            &pk_bytes,
                            predicates,
                            order_by,
                            schema_opt.as_ref(),
                            context,
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
                            .scan_partition(table, &pk_bytes, schema_opt.as_ref())
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
                            .scan_partition(table, pk_bytes, schema_opt.as_ref())
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
                    self.storage
                        .scan(table, None, None, None, schema_opt.as_ref())
                        .await?
                }
            };

            log::info!("Scan returned {} rows", scan_results.len());

            for (key, value) in scan_results {
                context.rows_processed += 1;
                context.scan_rows += 1;

                // build_row_from_scan returns None for tombstoned/null rows (Issue #191).
                let Some(row) = build_row_from_scan(key, value, projection, schema_opt.as_ref())
                else {
                    continue;
                };

                if evaluate_predicates(&row, predicates)? {
                    results.push(row);
                }

                if results.len() > MAX_RESULTS {
                    return Err(Error::query_execution(
                        "Result set too large, consider adding LIMIT".to_string(),
                    ));
                }
            }
        }

        Ok(results)
    }
}
