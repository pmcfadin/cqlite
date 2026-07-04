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
    honest_targeted_path, parse_table_id, partition_key_digest, select_has_writetime_ttl,
    sort_rows_by_token, validate_token_predicates, PartitionLookupOutcome, SSTablePredicate,
};
use super::{
    AccessPath, ColumnInfo, ExecutionContext, ExecutionStep, FallbackReason, OptimizedQueryPlan,
    ProjectionFlags, QueryResult, QueryRow, Result, SelectExecutor, StorageEngine, TableId,
    TableSchema,
};
use crate::query::result_budget::{enforce_result_budget, estimate_query_row_bytes};
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
            return self.execute_constant_query(&plan.statement, &context);
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

        // Issue #1582 (FINDING 2): if the plan's only post-scan steps are LIMIT
        // and projection, the materializing scan may stop collecting once it has
        // `offset + count` rows — the exact set a later LIMIT keeps — so a LIMITed
        // query over a wide table is honored WITHOUT the byte budget tripping on
        // matching rows beyond the limit. Any reordering/reducing step (Sort, PER
        // PARTITION LIMIT, Aggregate, Filter) makes early-stop unsafe, so the scan
        // then collects the full (still byte-budget-bounded) result.
        let scan_collect_bound = compute_scan_collect_bound(&execution_steps);

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
                            scan_collect_bound,
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
                    // roborev FINDING B: when the materializing scan early-stopped
                    // (`scan_collect_bound` is `Some`), it ALREADY applied this
                    // LIMIT's OFFSET — skipping the leading matching rows uncharged
                    // and collecting only `count`. Re-applying the OFFSET here would
                    // wrongly drop the first `offset` of the already-offset rows, so
                    // pass `None`: this step then only truncates to `count` (a
                    // safety net; the scan already bounded the row count). When
                    // early-stop was disallowed (`None`), the OFFSET is applied here
                    // as before.
                    let effective_offset = if scan_collect_bound.is_some() {
                        None
                    } else {
                        *offset
                    };
                    intermediate_results = self.execute_limit(
                        intermediate_results,
                        *count,
                        effective_offset,
                        &mut context,
                    )?;
                }
                ExecutionStep::Project { columns } => {
                    intermediate_results =
                        self.execute_projection(intermediate_results, columns, &mut context)?;
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

    /// Background task: Execute streaming scan and send rows through channel
    pub(super) async fn execute_streaming_background(
        storage: Arc<StorageEngine>,
        // Issue #1587 (E5): schema resolved ONCE per query by `execute_streaming`
        // and moved into this task — no per-scan-step registry lock + deep clone.
        query_schema: Option<Arc<TableSchema>>,
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
        // key, so we track the current partition and reset the counter at each
        // boundary. Issue #1590 (E8): the boundary is compared on the partition
        // key's 128-bit `partition_key_digest` (a heap-free hash of the key bytes
        // we already hold) as a FAST pre-check, then confirmed by EXACT byte
        // equality against the current partition's raw bytes — stored ONCE when
        // the boundary advances, never cloned per row. This keeps correctness
        // independent of digest collisions (a collision between two DISTINCT
        // partitions never shares a counter).
        let per_partition_limit = execution_steps.iter().find_map(|step| match step {
            ExecutionStep::PerPartitionLimit { count } => Some(*count),
            _ => None,
        });
        let mut current_partition: Option<(u128, Vec<u8>)> = None;
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
                    let schema_opt = query_schema.as_deref();

                    // FINDING 2 (Issue #955 follow-up): reject a `token(...)` whose
                    // columns are not the full partition key in declared order
                    // before scanning (same rule as the materializing path).
                    validate_token_predicates(predicates, schema_opt)?;

                    // Issue #949: a fully-constrained `WHERE pk = ?` is served by a
                    // partition-targeted lookup that prunes SSTables via bloom/BTI,
                    // instead of streaming a scan over every SSTable. The resulting
                    // rows are sent through the same per-row pipeline below
                    // (predicates, PER PARTITION LIMIT, OFFSET, LIMIT). Note
                    // `scan_partition` reconciles across SSTable generations like the
                    // materializing `scan()` (last-write-wins + tombstone shadowing),
                    // which is the authoritative read semantics; it does not merely
                    // mirror `scan_stream`'s per-key merge.
                    let lookup = classify_partition_lookup(predicates, schema_opt);
                    if let PartitionLookupOutcome::Targeted(ref pk_bytes) = lookup {
                        // Issue #960: the streaming analogue of the materializing
                        // partition-targeted lookup. Epic #951 (honest paths): the
                        // `tombstones` build's `scan_partition` is a full-scan +
                        // retain with NO prune, reported via `engaged == false`; only
                        // claim `StreamingPartitionLookup` when it really pruned.
                        let (rows, engaged) =
                            storage.scan_partition(table, pk_bytes, schema_opt).await?;
                        crate::query::access_path::record(honest_targeted_path(
                            AccessPath::StreamingPartitionLookup,
                            engaged,
                        ));
                        for (key, value) in rows {
                            let part_sig =
                                per_partition_limit.map(|_| partition_key_digest(&key.0));
                            let Some(row) = build_row_from_scan(key, value, projection, schema_opt)
                            else {
                                continue;
                            };
                            if !evaluate_predicates(&row, predicates)? {
                                continue;
                            }
                            if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                                // Fast digest pre-check, then EXACT byte confirm so a
                                // digest collision between DISTINCT partitions never
                                // shares a counter (issue #1590).
                                let same = matches!(
                                    &current_partition,
                                    Some((d, bytes))
                                        if *d == sig && bytes.as_slice() == row.key.0.as_slice()
                                );
                                if !same {
                                    // Clone the key bytes ONCE per boundary, not per row.
                                    current_partition = Some((sig, row.key.0.clone()));
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
                            let (rows, engaged) =
                                storage.scan_partition(table, pk_bytes, schema_opt).await?;
                            all_engaged &= engaged;
                            combined.extend(rows);
                        }
                        crate::query::access_path::record(honest_targeted_path(
                            AccessPath::MultiPartitionLookup,
                            all_engaged,
                        ));
                        sort_rows_by_token(&mut combined);
                        for (key, value) in combined {
                            let part_sig =
                                per_partition_limit.map(|_| partition_key_digest(&key.0));
                            let Some(row) = build_row_from_scan(key, value, projection, schema_opt)
                            else {
                                continue;
                            };
                            if !evaluate_predicates(&row, predicates)? {
                                continue;
                            }
                            if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                                // Fast digest pre-check, then EXACT byte confirm so a
                                // digest collision between DISTINCT partitions never
                                // shares a counter (issue #1590).
                                let same = matches!(
                                    &current_partition,
                                    Some((d, bytes))
                                        if *d == sig && bytes.as_slice() == row.key.0.as_slice()
                                );
                                if !same {
                                    // Clone the key bytes ONCE per boundary, not per row.
                                    current_partition = Some((sig, row.key.0.clone()));
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
                        .scan_stream(table, None, None, schema_opt, buffer_size)
                        .await?;

                    while let Some(item) = scan_stream.recv().await {
                        let (key, value) = item?;
                        // Capture the partition-key digest before `key` is moved
                        // into row construction (only when needed).
                        let part_sig = per_partition_limit.map(|_| partition_key_digest(&key.0));
                        let Some(row) = build_row_from_scan(key, value, projection, schema_opt)
                        else {
                            continue;
                        };

                        if !evaluate_predicates(&row, predicates)? {
                            continue;
                        }

                        // Apply PER PARTITION LIMIT: cap matching rows per
                        // partition, before OFFSET/LIMIT (Cassandra semantics).
                        if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                            // Fast digest pre-check, then EXACT byte confirm so a
                            // digest collision between DISTINCT partitions never
                            // shares a counter (issue #1590).
                            let same = matches!(
                                &current_partition,
                                Some((d, bytes))
                                    if *d == sig && bytes.as_slice() == row.key.0.as_slice()
                            );
                            if !same {
                                // Clone the key bytes ONCE per boundary, not per row.
                                current_partition = Some((sig, row.key.0.clone()));
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
    ///
    /// Issue #1582 (D6): a running byte estimate ([`estimate_query_row_bytes`]) is
    /// accumulated as rows are collected and fails fast with
    /// [`Error::ResultTooLarge`] once the configured budget is crossed, so an
    /// oversized result is rejected with an actionable message (add `LIMIT` /
    /// stream) rather than silently materializing an unbounded `Vec`.
    ///
    /// `collect_bound` (FINDING 2 + roborev FINDING B) bounds the MATCHING
    /// (post-predicate) rows: `Some(ScanCollectBound { count, offset })` when the
    /// caller determined the plan's post-scan steps cannot reorder or drop rows.
    /// The scan then SKIPS the first `offset` matching rows WITHOUT pushing them
    /// or charging their bytes, and collects the next `count` — exactly what a
    /// later LIMIT keeps. Skipping the offset rows uncharged means a small final
    /// result never fails `ResultTooLarge` because the discarded offset rows were
    /// wide. The skip counts only rows that PASSED `evaluate_predicates`, composing
    /// with FINDING 1 (a raw pre-predicate skip would drop matching rows).
    ///
    /// FINDING 1: `collect_bound` is pushed into the underlying `storage.scan`
    /// `limit` ONLY for a pure unfiltered scan (no executor-side predicate to
    /// evaluate). `storage.scan` is not predicate-aware, so pushing the bound when
    /// this step carries folded predicates (e.g. `WHERE non_pk_col = ?`, which the
    /// optimizer places in `predicates` WITHOUT a residual `Filter`) would return
    /// only the first `offset + count` RAW rows and silently drop matching rows
    /// further along the scan. The executor-side early-stop remains correct in both
    /// cases because it counts only rows that already passed the predicate. Also,
    /// a `count == 0` bound (LIMIT 0) short-circuits to an empty result before any
    /// scan or budget work on every path (targeted, multi-targeted, fallback).
    ///
    /// NOTE (FINDING 1 / peak memory): the underlying `storage.scan` still
    /// materializes each SSTable's matching rows via the reader's index path before
    /// this method sees them, so for an UNBOUNDED (`collect_bound == None`)
    /// full scan the byte guard here is a fail-fast ceiling, not a peak-memory
    /// bound. The only incremental primitive (`scan_stream`) does not read
    /// CQLite-written single-generation SSTables (it returns zero rows via the
    /// non-index block path), so it is not a drop-in for this materializing path;
    /// truly bounding peak for an unbounded scan needs an index/BTI-aware
    /// incremental reader scan — tracked as a follow-up, out of scope for #1582.
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
        // Issue #1582 (FINDING 2 + roborev FINDING B): early-stop bound
        // (count + uncharged offset skip) when the plan permits it, else None.
        // See the method doc.
        collect_bound: Option<ScanCollectBound>,
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        // Issue #1582 (FINDING 2): a LIMIT 0 collects nothing. Short-circuit
        // BEFORE any scan, lookup, row build, predicate eval, push, or byte-budget
        // work on EVERY path (targeted, multi-targeted, fallback scan). Besides
        // being the correct empty result, this stops the targeted/wide path from
        // pushing a first matching row and byte-checking it — which could raise
        // `ResultTooLarge` instead of returning empty. A `count == 0` bound is set
        // only when the plan's post-scan steps cannot reorder/drop rows, so an
        // empty result is result-preserving here (LIMIT 0 is empty regardless of
        // any OFFSET).
        if matches!(collect_bound, Some(b) if b.count == 0) {
            return Ok(Vec::new());
        }

        // roborev FINDING B: number of leading MATCHING rows to skip uncharged
        // (the OFFSET) and the number of matching rows to RETURN (the LIMIT).
        // `None` when the plan disallows early-stop → skip nothing, collect all.
        let skip_count = collect_bound.map(|b| b.offset).unwrap_or(0);
        let take_count = collect_bound.map(|b| b.count);
        let mut skipped: usize = 0;

        // Issue #1582 (FINDING 1): the underlying `storage.scan` /
        // `scan_with_cell_metadata` receive a `None` key range and are NOT
        // predicate-aware, so any `SSTableScan` predicate (e.g. `WHERE
        // non_pk_col = ?`, which the optimizer folds INTO this step's `predicates`
        // WITHOUT a residual `Filter`) is enforced ONLY by the per-row
        // `evaluate_predicates` below. Pushing `offset + count` into storage before
        // that evaluation would return just the first `offset + count` RAW
        // (unfiltered) rows and silently drop matching rows further along the scan
        // → WRONG RESULTS. So push a storage-layer row limit ONLY for a pure
        // unfiltered scan (no executor-side predicate to evaluate). When unsure
        // whether storage enforces a pushed predicate, treat it as NOT enforced.
        // The executor-side early-stop (below) stays safe in BOTH cases: it stops
        // only after a row has passed `evaluate_predicates`.
        // For a pure unfiltered scan, storage must still yield offset + count raw
        // rows (all of which match) so the executor can skip `offset` and return
        // `count`. roborev FINDING B: the offset rows are skipped uncharged AFTER
        // storage returns them; `storage.scan` is not predicate-aware, so this
        // push is withheld the moment executor-side predicates exist.
        let storage_limit = if predicates.is_empty() {
            collect_bound.map(|b| b.offset.saturating_add(b.count))
        } else {
            None
        };

        // Issue #1582 (D6): a ROW COUNT is the wrong unit for a memory guard —
        // 1M skinny rows may fit while a few thousand wide rows blow the <128MB
        // target. The primary guard is a running BYTE estimate (below) against
        // the configured budget; the row count remains only as a secondary
        // safety valve, sourced from the load-bearing `max_result_rows` config
        // knob (roborev FINDING A — NOT a hardcoded constant).
        let max_rows = self.max_result_rows;
        let byte_budget = self.max_result_bytes;
        // Running estimate of the materialized result's logical size, accumulated
        // with the SAME estimator the row cache uses (issue #1582).
        let mut result_bytes: usize = 0;

        log::info!(
            "Executing SSTableScan: table=\"{}\", predicates={:?}, include_cell_metadata={}",
            table,
            predicates,
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

        // FINDING 2 (Issue #955 follow-up): a `token(...)` predicate is evaluated
        // by hashing the row's raw partition key, so its argument columns MUST be
        // the full partition key in declared order or the result is silently
        // wrong. Reject (Cassandra-style) before scanning/evaluating.
        validate_token_predicates(predicates, schema_opt)?;

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
                    // Issue #1582 (FINDING 1): push the LIMIT bound into the
                    // metadata scan ONLY for a pure unfiltered scan (`storage_limit`
                    // is `None` when executor-side predicates must still run — see
                    // the non-metadata Fallback arm and the `storage_limit`
                    // derivation above). The executor-side early-stop below still
                    // bounds collection after predicate evaluation.
                    self.storage
                        .scan_with_cell_metadata(table, None, None, storage_limit, schema_opt)
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
                    // roborev FINDING B: skip the first `offset` MATCHING rows
                    // uncharged — never push them nor add their bytes — so an
                    // OFFSET's discarded rows cannot trip the byte budget.
                    if skipped < skip_count {
                        skipped += 1;
                        continue;
                    }
                    result_bytes = result_bytes.saturating_add(estimate_query_row_bytes(&row));
                    results.push(row);
                    enforce_result_budget(&results, result_bytes, byte_budget, max_rows)?;
                    // FINDING 2: early-stop at the LIMIT (`count`) bound where safe.
                    // NOTE (FINDING 1 scope): the metadata (WRITETIME/TTL) full-scan
                    // fallback above materializes via `scan_with_cell_metadata`
                    // because there is no streaming metadata scan yet — the byte
                    // guard here still fails-fast on the collected result, and the
                    // early-stop avoids building surplus rows, but the underlying
                    // metadata scan is not incrementally bounded. A streaming
                    // metadata scan (the metadata analog of `scan_stream`) is a
                    // documented follow-up; the dominant, unbounded risk is the
                    // non-metadata full scan, which IS bounded above.
                    if let Some(bound) = take_count {
                        if results.len() >= bound {
                            break;
                        }
                    }
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
                    // Issue #1582 (FINDING 1): push the query-wide LIMIT bound
                    // (offset + count) INTO the scan ONLY for a pure unfiltered scan
                    // (`storage_limit`). `storage.scan` is not predicate-aware, so
                    // when this step carries executor-evaluated predicates
                    // `storage_limit` is `None` — pushing a raw-row limit ahead of
                    // `evaluate_predicates` would drop matching rows past the first
                    // `offset + count` RAW rows. The executor-side early-stop below
                    // still bounds collection after predicate evaluation, so a
                    // LIMITed query is served without the byte budget biting on
                    // matching rows beyond the limit.
                    self.storage
                        .scan(table, None, None, storage_limit, schema_opt)
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
                    // roborev FINDING B: skip the first `offset` MATCHING rows
                    // uncharged — never push them nor add their bytes — so an
                    // OFFSET's discarded rows cannot trip the byte budget.
                    if skipped < skip_count {
                        skipped += 1;
                        continue;
                    }
                    result_bytes = result_bytes.saturating_add(estimate_query_row_bytes(&row));
                    results.push(row);
                    enforce_result_budget(&results, result_bytes, byte_budget, max_rows)?;
                    // FINDING 2: the targeted / multi-partition paths return a Vec
                    // already bounded by the partition(s); early-stop at `count`
                    // still avoids building rows a later LIMIT would discard.
                    if let Some(bound) = take_count {
                        if results.len() >= bound {
                            break;
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}

/// Early-stop collection bound for the materializing scan (issue #1582).
///
/// `count` is the number of MATCHING (post-predicate) rows the scan RETURNS —
/// the LIMIT — and `offset` is the number of leading matching rows the scan
/// SKIPS (the OFFSET) WITHOUT pushing them or charging their bytes to the budget
/// (roborev FINDING B). Skipping the offset rows uncharged means a small final
/// result never fails `ResultTooLarge` merely because the discarded offset rows
/// were wide.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ScanCollectBound {
    /// LIMIT: matching rows to collect + return after the offset skip.
    count: usize,
    /// OFFSET: leading matching rows to skip, uncharged, before collecting.
    offset: usize,
}

/// Compute the early-stop collection bound for the materializing scan (issue
/// #1582 / FINDING 2 + roborev FINDING B).
///
/// Returns `Some(ScanCollectBound { count, offset })` ONLY when the plan's
/// post-scan steps are limited to `Limit` and `Project`, i.e. no step (`Sort`,
/// `PerPartitionLimit`, `Aggregate`, `Filter`) can reorder or drop rows. In that
/// case the scan may skip the first `offset` matching rows uncharged and collect
/// the next `count` — exactly the set a later `Limit { count, offset }` keeps —
/// so early-stopping is result-preserving AND keeps the byte budget from tripping
/// on the skipped offset rows or on matching rows beyond the limit. Because the
/// scan then fully applies the LIMIT/OFFSET, the caller neutralizes the OFFSET in
/// the downstream `Limit` step (it re-applying the offset would drop the first
/// `offset` of the already-offset rows). Any other step returns `None` (collect
/// the full, still byte-budget-bounded result). Absent a `Limit`, returns `None`.
fn compute_scan_collect_bound(steps: &[ExecutionStep]) -> Option<ScanCollectBound> {
    let mut bound: Option<ScanCollectBound> = None;
    for step in steps {
        match step {
            ExecutionStep::SSTableScan { .. } | ExecutionStep::Project { .. } => {}
            ExecutionStep::Limit { count, offset } => {
                // `count`/`offset` are u64; saturate into usize for the in-memory
                // collection bounds.
                bound = Some(ScanCollectBound {
                    count: usize::try_from(*count).unwrap_or(usize::MAX),
                    offset: usize::try_from(offset.unwrap_or(0)).unwrap_or(usize::MAX),
                });
            }
            // Any other step may reorder or drop rows; early-stopping the scan
            // at the raw match count would then yield the wrong result set.
            _ => return None,
        }
    }
    bound
}

// The byte-budget estimator + enforcement live in `crate::query::result_budget`
// (shared verbatim with the legacy engine point-lookup path, issue #1582 / D6);
// this module reuses them via the `use` alias below rather than forking the logic.
