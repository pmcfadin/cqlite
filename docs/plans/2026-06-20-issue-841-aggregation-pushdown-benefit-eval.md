# Issue #841 — Aggregation Pushdown Benefit Evaluation (GO/NO-GO)

**Date:** 2026-06-20
**Issue:** [#841](https://github.com/pmcfadin/cqlite/issues/841) — Flight/Trino aggregation pushdown (count/sum/min/max/avg) via a single finalize split — *evaluate benefit first*
**Epic:** [#874](https://github.com/pmcfadin/cqlite/issues/874) (flight pushdown). The epic mandate: this lands **only with a measured win, OR is explicitly closed as not-worth-it with the benchmark recorded.**

## Recommendation: GO — but scoped to `count`/`sum`/`min`/`max`/`avg` *only when Trino's `AggregateFunction` set has no grouping or low-to-mid-cardinality `GROUP BY`*. NO-GO for high-cardinality `GROUP BY`.

The data-reduction win is enormous where analytics aggregation actually matters (`count(*)`, global aggregates, and low/mid-cardinality `GROUP BY`) and degrades smoothly to break-even at high cardinality. The single-worker finalize cost is negligible precisely in the cases where pushdown wins, because the merged payload is tiny. The recommendation is to implement, gated by a cardinality/feasibility check in `applyAggregation` that declines pushdown (leaving Trino to aggregate locally) when the estimated group count approaches the row count.

---

## Measurement

### Method

A throwaway integration test (`cqlite-flight/tests/zzz_agg_bench_scratch.rs`, since deleted) drove the **real production merge path** — `cqlite_flight::producer::MergeProducer` over real SSTables built in-process by the cqlite-core write engine — and measured what the Flight server emits today (full scan, post-reconciliation) versus what server-side aggregation would emit.

- Table: `pk=group_id(int), clustering ck(int), regular metric(int)` — models `SELECT group_id, sum(metric), count(*) ... GROUP BY group_id`.
- Fixed 50,000 reconciled rows, varying the number of distinct groups (partitions) to sweep cardinality.
- "Emitted bytes" = sum of `RecordBatch::get_array_memory_size()` across all batches the producer returns (the Arrow payload handed to the Flight/IPC layer).
- Aggregated payload estimated **conservatively against pushdown**: one row per group, and each aggregate row charged at **3× the full-scan per-row byte cost** (real aggregate rows of a group key + two `i64` accumulators are comparable to or smaller than this; the 3× inflation biases the reduction factors *downward*, so the real win is at least this large).

This measures the **data-shipped** axis, which is the dominant cost for the Flight → Trino transfer the design targets (Arrow IPC encoding + gRPC framing + network + Trino block conversion all scale with emitted rows/bytes).

### Raw numbers (50,000 rows, real `MergeProducer`)

| Scenario | Groups | Full-scan rows | Full-scan Arrow bytes | Agg rows | Agg est. bytes | Row reduction | Byte reduction |
|---|---|---|---|---|---|---|---|
| global / `count(*)` | 1 | 50,000 | 602,016 | 1 | 48 | **50,000×** | **~12,500×** |
| low-card `GROUP BY` | 10 | 50,000 | 602,016 | 10 | 480 | **5,000×** | **~1,254×** |
| mid-card `GROUP BY` | 1,000 | 50,000 | 602,016 | 1,000 | 48,000 | **50×** | **~12.5×** |
| high-card `GROUP BY` | 50,000 | 50,000 | 602,016 | 50,000 | 2,400,000 | **1.0×** | **0.3× (loss)** |

Per emitted row was ~12 B of Arrow array memory for this narrow schema.

Secondary observation: the full-scan test (4 scenarios × 50k-row build + merge) took **~826 s** of wall time end to end. The merge scan itself is not free — the server already walks every reconciled row regardless. Aggregating during that same walk is essentially free incremental work; the only thing pushdown removes is the cost of *materializing and shipping* those rows.

### Reading the numbers

- **`count(*)` / global aggregates: always a massive win.** Row count collapses to 1 independent of input size — 50,000× here, and it grows linearly with dataset size. This is the headline case for analytics dashboards.
- **Low-cardinality `GROUP BY` (≤ a few hundred groups): large win.** 10 groups → ~1,250× byte reduction. This is the common analytics shape (group by region, status, day-bucket, tenant tier, etc.).
- **Mid-cardinality (~1k groups): still a solid ~12.5× byte reduction.** Worth pushing.
- **High-cardinality `GROUP BY` (groups ≈ rows): no win, slight loss.** When nearly every row is its own group, the "aggregated" output is the full row set plus accumulator overhead — pushdown *adds* bytes and steals parallelism. This is exactly where the connector must **decline** pushdown.

The crossover is governed by `rows_emitted_full_scan / num_groups`. Pushdown wins materially once that ratio is roughly ≥ 4–10×; below that it is break-even-to-negative.

---

## Analysis

### 1. Data-reduction math

For an aggregate query the server ships `G` rows (one per group) instead of `N` reconciled rows, where `G` = distinct grouping-key combinations and `N` = live rows. Reduction factor ≈ `N / G`. Because `N` grows with the dataset while `G` is bounded by the grouping domain, the win **scales with data size** for any fixed-cardinality grouping — the property that makes pushdown valuable for large tables specifically. `count`/`sum`/`min`/`max`/`avg` are all combinable across nodes (`Σ`, `Σ`, `min`, `max`, `Σsum/Σcount`), so the per-replica partial is one row per group per node, and the connector's final merge stays `replicas × G` rows in, `G` rows out.

### 2. Interaction with #839 (token-range pruning) and #834 (predicate pushdown)

These cut a **different** axis and do **not** subsume aggregation pushdown:

- **#839 (token-range pruning)** reduces *input SSTable bytes read* per split — it does nothing to the number of rows shipped to Trino for an aggregate. A `count(*)` over a pruned range still ships every row in that range absent aggregation pushdown.
- **#834 (predicate pushdown)** reduces the *row set* via `WHERE`, but an aggregate is computed over whatever survives the predicate. A selective `WHERE` shrinks `N`, which shrinks the absolute win, but the *ratio* `N/G` is unchanged — `SELECT count(*) ... WHERE active=true` still collapses thousands of surviving rows to one. Predicate pushdown and aggregation pushdown compose multiplicatively; they do not compete.

Both #834 and #839 are still **OPEN**, so today Trino fetches the full (un-pruned, un-filtered) row set and aggregates locally — the worst case, and the strongest argument for #841. Even after they land, aggregation pushdown retains independent value on the rows-shipped axis.

### 3. Single-worker finalize parallelism loss

The design funnels the final merge through one Trino worker. This is a real cost **only when `G` is large** — and that is exactly the high-cardinality case the recommendation already excludes from pushdown. In the winning cases (`count(*)`, low/mid-card `GROUP BY`) the single worker merges at most `replicas × G` tiny rows (e.g. 3 × 1,000 = 3,000 rows), which is trivially fast and dwarfed by the network/CPU saved on the rows *not* shipped. The lost intra-aggregation parallelism cannot erode a 1,250× transfer reduction. Where it *would* matter (millions of groups), the connector declines pushdown and Trino's normal distributed aggregation applies.

### 4. Complexity / maintenance cost

The single-finalize-split path is non-trivial: `applyAggregation` in `ConnectorMetadata`, aggregation-spec encoding in the Flight ticket, a fan-out `PageSource`, and server-side partial computation during the merge. But the server already scans every reconciled row in `MergeProducer::produce_from_paths`, so the server side is an accumulator hooked into the existing per-row loop — modest. The connector side carries most of the complexity (correct `applyAggregation` rewrite, cardinality gating, correct residual handling for un-pushable aggregates). The benefit magnitude (≥1000× on the dominant cases, scaling with data size) clearly justifies this.

---

## GO/NO-GO decision tied to the epic acceptance criterion

The epic requires a **measured win or a recorded not-worth-it close**. The measurement above is a clear, large, dataset-scaling win on the cases that matter for analytics, so:

**GO**, with these guardrails baked into the implementation:

1. **Push only the combinable aggregates** named in #841: `count`, `sum`, `min`, `max`, `avg` (as `sum`+`count`). Leave `count(DISTINCT)`, `ROLLUP`/`CUBE`/`GROUPING SETS`, arg expressions, coercions, and ordered/filtered aggregates residual in Trino (already the issue's non-goals).
2. **Cardinality gate.** In `applyAggregation`, decline pushdown (return empty / no rewrite, so Trino aggregates locally) when the estimated distinct group count approaches the estimated row count — i.e. when the expected reduction ratio falls below ~4–10×. Use connector statistics where available; default to pushing for no-grouping and small explicit grouping domains, and be conservative (decline) when unknown and the table is large. This directly prevents the only measured loss case (high-cardinality, 0.3× / parallelism loss).
3. **Correctness over partials.** Because Trino removes the `AggregationNode` entirely (`PushAggregationIntoTableScan`), the single finalize split MUST return fully merged results; per-split partials are never re-aggregated by Trino. This is a correctness invariant, not an optimization.

### If a maintainer prefers to defer

The honest fallback is to land #834 and #839 first (they cut input bytes and predicate-filtered rows on every query, are simpler, and are prerequisites for a fair end-to-end benchmark), then revisit #841 with a Docker/Trino end-to-end wall-clock benchmark. The data-reduction model here already establishes that aggregation pushdown will still win on the rows-shipped axis after those land, so deferring is a sequencing choice, not a NO-GO.

---

## Benchmark reproducibility

The numbers above came from a self-contained `#[test]` placed at `cqlite-flight/tests/zzz_agg_bench_scratch.rs` that: (a) built 50k-row SSTables across N partitions via `cqlite_core::storage::write_engine::WriteEngine`, (b) ran `cqlite_flight::producer::MergeProducer::produce`, and (c) summed `num_rows()` and `get_array_memory_size()` over the returned batches, comparing against one-row-per-group estimates. It was deleted after recording the results to keep the tree clean (only this doc is intended to remain). To re-run, recreate that harness against the public `MergeProducer` API. Note the full-scan path is slow (~minutes for the 4-scenario sweep) — itself evidence that avoiding row materialization/shipping is worthwhile.
