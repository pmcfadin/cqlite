//! Streaming SELECT producer (`execute_streaming_background`) — issue #1578 split.
//!
//! Relocated verbatim from `execute.rs` (epic #1116 file-size split) so the
//! materializing runner and the streaming producer live in separate files. As a
//! child module of `select_executor` this reaches `mod.rs`'s private items
//! directly; logic, ordering, and error handling are unchanged.

use super::{
    build_row_from_scan, classify_partition_lookup, evaluate_predicates, honest_targeted_path,
    partition_key_digest, sort_rows_by_token, validate_token_predicates, PartitionLookupOutcome,
};
use super::{
    AccessPath, ExecutionStep, QueryRow, Result, SelectExecutor, StorageEngine, TableId,
    TableSchema,
};
use std::sync::Arc;
use tokio::sync::mpsc;

impl SelectExecutor {
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
                                        if *d == sig && bytes.as_slice() == row.key.as_bytes()
                                );
                                if !same {
                                    // Clone the key bytes ONCE per boundary, not per row.
                                    current_partition = Some((sig, row.key.as_bytes().to_vec()));
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
                                        if *d == sig && bytes.as_slice() == row.key.as_bytes()
                                );
                                if !same {
                                    // Clone the key bytes ONCE per boundary, not per row.
                                    current_partition = Some((sig, row.key.as_bytes().to_vec()));
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
                    // Issue #1592: consume the BATCHED streaming surface — one
                    // async wake per batch, not per row. Flattening each batch
                    // yields the same rows in the same order as `scan_stream`.
                    let mut scan_stream = storage
                        .scan_stream_batched(table, None, None, schema_opt, buffer_size)
                        .await?;

                    while let Some(batch) = scan_stream.recv().await {
                        for (key, value) in batch? {
                            // Capture the partition-key digest before `key` is moved
                            // into row construction (only when needed).
                            let part_sig =
                                per_partition_limit.map(|_| partition_key_digest(&key.0));
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
                                        if *d == sig && bytes.as_slice() == row.key.as_bytes()
                                );
                                if !same {
                                    // Clone the key bytes ONCE per boundary, not per row.
                                    current_partition = Some((sig, row.key.as_bytes().to_vec()));
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
                }
                ExecutionStep::Limit { .. } | ExecutionStep::PerPartitionLimit { .. } => {
                    // Enforced inline during the scan above (see the bounds
                    // extracted before the loop).
                }
                // Projection and predicate filtering are pushed into SSTableScan above.
                ExecutionStep::Project { .. } | ExecutionStep::Filter { .. } => {}
                _ => {
                    // Data-safety (issue #1694): log the step's variant name only,
                    // never its contents (which carry query literals/values).
                    log::warn!(
                        "Streaming execution: skipping unsupported step {}",
                        step.variant_name()
                    );
                }
            }
        }

        Ok(())
    }
}
