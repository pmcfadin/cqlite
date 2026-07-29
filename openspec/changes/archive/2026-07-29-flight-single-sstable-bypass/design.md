# Design: single-SSTable merge bypass on the Flight `do_get` data plane (issue #3058)

## Context

### The path as it exists today

```
service.rs:553  do_get
  → :764  do_get_inner
  → :981  do_get_resolve           builds producer (:1001), warm readers (:1049)
                                   [aggregate tickets return early at :1028 → DoGetInput::Aggregate]
  → :918-933  DoGetInput::Rows
  → streaming.rs:343  spawn_streaming_from_readers
  → streaming.rs:375  spawn_streaming
  → producer_warm.rs:44  MergeProducer::produce_streaming_from_readers
       :52   early return when is_aggregating()
       :56   prune_readers()  → `readers` (token-pruned, zero I/O)
       :75   point-read route when a full-PK-equality predicate is pushed
       :110-117  KWayMerger::new_from_readers(readers, ...).with_now_secs(...)   ← UNCONDITIONAL
  → producer_stream.rs:165  drive_merge_over
  → write_engine/merge/streaming.rs:610  step_streaming
  → write_engine/merge/mod.rs  reconcile_cluster_with_overlap_counted
```

`reconcile_cluster_with_overlap_counted` is the compaction reconciler. Reaching it forces the
per-input rows to be decoded by `CompactionPolicy::on_data_row`
(`row_decoder/compaction.rs:585`), which passes `want_cell_metadata = true` (`:600`), so
`row_data.rs:74` allocates `Some(HashMap::new())` per row and inserts `column.name.clone()` per cell
(`row_data.rs:585`, `:757`). Nothing on the Flight read path ever reads `cell_metadata` — every
`QueryRow` it builds sets it to `None` (`filter.rs:570,579`; `agg.rs:467,738,793`).

### The path the bare scan uses (367,760 rows/s)

`SSTableManager::scan_stream_batched` (`sstable/mod.rs:2312`) short-circuits at `:2321-2331`:

```rust
let readers = self.resolve_table_readers(table_id).await;
if readers.len() == 1 {
    if let Some(reader) = readers.into_iter().next() {
        return Ok(reader.scan_stream_batched(/* … */));
    }
}
```

`SSTableReader::scan_stream_batched` (`reader/data_access/sequential.rs:518`) delegates to
`scan_stream_batched_admitted(..., ScanAdmission)` (`:539`) → `run_scan_stream_batched` →
`run_scan_stream_windowed` (`reader/scan_stream_windowed.rs:408`), whose window drain builds its
parser with **`read_shadowing = true`** (`scan_stream_windowed.rs:748-751`, comment cites #1741:
"single-gen full-scan read path applies SELECT-semantic read shadowing").

Correctness on that non-merge path is carried by **`PartitionShadow`**
(`row_decoder/partition_shadow.rs:44`; `open()` `:68`; `cell_shadowed_or_expired` `:255`), enabled
via `with_read_shadowing` (`row_decoder/mod.rs:988`). It covers partition deletions, range
tombstones (RT-marker FSM feed `block_emit_windowed.rs:346-380`, RT priming `:237-249`), and TTL
expiry through the same `now_clock` seam (`now_clock.rs:36,61,66`, honoring
`CQLITE_TTL_NOW_OVERRIDE_SECS`). Static cells are accumulated and injected into every clustering row
at `block_emit_windowed.rs:301-307`.

So the fast path is not a "skip reconciliation" path — it is a **different, already-shipped
implementation of read-time reconciliation** that is correct for exactly one generation, which is
precisely the case at hand.

### The five semantic gaps a naive bypass would drop

Enumerated here because the recommendation's risk is entirely in this list.

| # | Gap | Status after investigation |
|---|---|---|
| (a) | `read_shadowing` must be `true` (compaction/verify parsers set it false: `full_index_scan.rs:140`, `block_emit_windowed.rs:817`) | Already `true` on `run_scan_stream_windowed` (`:748-751`). Must be **asserted**, not assumed. |
| (b) | `entry_to_row` (`producer.rs:1188`) does collection-element reassembly (`assemble_read_cells`) and the row-marker liveness / `has_live_data_cell` visibility rule (`producer.rs:1199-1220`, #2374/#2789) | `ScanRow::Row` already carries **collapsed** collections (the single-generation reader's shape — the very shape `assemble_read_cells` is documented as mirroring, `producer.rs:1224-1228`), so reassembly is unnecessary. The liveness rule is the real work item: the single-gen decoder decides emittability itself, and the two decisions must be proven equivalent by differential test, not by reading. |
| (c) | `RowData::Tombstone → Ok(None)` row suppression (`producer.rs:1202`) | Covered natively: a row tombstone reaches the fast path as `ScanRow::Marker`, and `build_row_from_scan_cached` suppresses it via `row.into_cells()?` (`row_build.rs:234-239`, issue #505). |
| (d) | Dropped-column filtering (reconcile Step 3b) | **See "Gap (d) resolved" below.** |
| (e) | Impedance: fast path yields `(RowKey, ScanRow)`; `entry_to_row` consumes `MergeEntry`/`RowData::Live{cells}` | Core already ships the adapter: `build_row_from_scan_cached` (`select_executor/row_build.rs:227`), used by the bare scan at `select_executor/streaming.rs:142,227,308`. Reuse it; do not write a second one. |
| (f) | Feature parity: ScanSpec predicate pushdown, projection, aggregation, token pruning, `max_batch_bytes`, cancellation, UDT registry, `now_secs` | Aggregation is **out of the branch entirely** (see below). The rest stay wired because Option A keeps `MergeProducer` as the shell. |

### Gap (d) resolved — dropped columns are TWO distinct mechanisms

1. **Physical** (a column present on disk but absent from the supplied schema): handled in the
   **shared** decoder at `row_data.rs:376-401` — the bytes are consumed with a synthetic
   `__dropped_col_N` column built from the authoritative on-disk header marshal type, and
   `emit = ctp.schema.is_some()` gates it out of the output. Both paths get this identically.
2. **Timestamp-based, reconcile Step 3b**: `ReconcileState::filter_dropped_columns`
   (`write_engine/merge/reconcile.rs:406`) purges a cell when `cell.timestamp <= drop_time` from
   `schema.dropped_columns: HashMap<String, i64>`, fed at `write_engine/merge/streaming.rs:578`.
   **`PartitionShadow` has NO counterpart** (zero `dropped_column` references in
   `partition_shadow.rs`) — this is a genuine divergence.

   **But it cannot fire on the Flight surface today.** The Flight producer builds its schema from
   the ticket's CQL DDL (`service.rs:479-490` → `cached_schema` → the CQL parser), and
   `cqlite-core/src/schema/cql_parser.rs:977` hardcodes `dropped_columns:
   std::collections::HashMap::new()`. The only surface that populates it is the JSON schema loader
   (`schema/aggregator/json.rs:251,329`), which Flight does not use. So `schema.dropped_columns` is
   **always empty** on `do_get`, and Step 3b is a no-op there today.

   That makes it a **latent** divergence, not a live one — which is exactly the shape that becomes a
   silent correctness bug the day a DDL/JSON schema path starts populating it. The design therefore
   makes `schema.dropped_columns.is_empty()` a **fail-closed precondition of the bypass**: non-empty
   → take the merger. That is an authoritative-metadata check on a schema field, not an inference.

### Gap (f) — aggregation confirmed out of scope of the branch

`do_get_resolve` returns early at `service.rs:1028` with `DoGetInput::Aggregate(paths)` when
`producer.is_aggregating()`, routing aggregates down the cold path (bounded per-group output). And
`produce_streaming_from_readers` itself early-returns `Ok(())` at `producer_warm.rs:52` on
`self.is_aggregating()`. The bypass site is therefore provably unreachable for aggregate tickets;
`producer.rs:587 with_aggregation` needs no work. The design must only ensure the bypass is placed
**after** that early return, not before it.

## Recommended design — Option A: source swap inside `MergeProducer`

Keep `MergeProducer` as the shell and, at `producer_warm.rs:110` (after the `is_aggregating`
early-return, after `prune_readers`, and after the point-read route), branch:

```
if pruned.len() == 1 && bypass_preconditions_hold(&self, &pruned[0]) && forced_path != Merge {
    → SingleSourceSource: reader.scan_stream_batched_admitted(...)   // (RowKey, ScanRow) batches
      adapter: build_row_from_scan_cached(key, row, projection, schema, &mut pk_cache)
} else {
    → KWayMerger::new_from_readers(...)                              // unchanged, today's code
}
```

Both arms then feed the **same** downstream drive loop that `drive_merge_over`
(`producer_stream.rs:165`) already implements — batching, `max_batch_bytes` (`batch_bytes.rs`),
`CancelFlag` polling, `ScanProgress`, `on_merger_built` phase-boundary fire, and the
`filter.rs` predicate/projection application. Concretely: factor the loop's row source behind a
small internal trait (`next_row() -> Option<Result<QueryRow>>`) with two implementations, rather
than duplicating the loop.

### Why A, tied to the PASS CONDITION

The pass condition is an **external** number: `do_get` ≥ ~280,000 rows/s/phys-core (within ~1.3x of
the 367,760 rows/s bare scan), reported as rows/s AND cycles/row. A drop in "% CPU in merge" with
unmoved rows/s is a FAIL — the #2877 shape.

Option A adopts **the exact code path that measured 367,760 rows/s**. The bare scan
(`execute_streaming`) is `SSTableManager::scan_stream_batched`'s `readers.len() == 1`
short-circuit + `build_row_from_scan_cached`; A puts the identical source and the identical adapter
under the Flight egress. The residual delta between A and 367,760 is therefore attributable to a
known, enumerable set (Arrow encode, the mpsc handoff, gRPC framing) rather than to an unmeasured
new path — which is the property that makes the kill criterion (below) *interpretable*.

### The bypass predicate (authoritative, fail-closed)

Taken at the decision point, all from authoritative state — never inferred from bytes:

1. `pruned.len() == 1`, where `pruned` is `prune_readers(readers)` (`producer_warm.rs:56,134`) over
   the warm registry's `GenerationSet` (`warm/identity.rs:96`) produced by
   `probe::probe_generation_set` (`warm/probe.rs:122`) from the authoritative `*-Data.db` listing.
2. `schema.dropped_columns.is_empty()` (gap (d)).
3. Not aggregating (already guaranteed by the `:52` early return; asserted, not re-derived).
4. Not the point-read route (already returned at `:75`).
5. `CQLITE_FLIGHT_MERGE_PATH` is not `merge`.

Every other condition → fall through to the merger. The predicate is **conjunctive and
fail-closed**: an unknown or unrepresentable condition takes the slow, known-correct arm.

### `now_secs` and the pinned clock

The merge arm threads `with_now_secs(Some(self.now_secs))` (`producer_warm.rs:115-117`, #2374/#2789).
The bypass arm must reach the identical clock: `PartitionShadow` reads TTL expiry through
`now_clock.rs:36,61,66`, which honors `CQLITE_TTL_NOW_OVERRIDE_SECS`. The producer's `now_secs` MUST
be threaded into the fast path's shadow clock explicitly — **not** left to ambient wall-clock — or
the query-semantics oracle's pinned `now` cannot be honored on the bypass. This is a first-class
task, not a detail.

### The forced-path escape hatch (how correctness is actually proven)

`CQLITE_FLIGHT_MERGE_PATH=bypass|merge` (unset = automatic), mirroring `CQLITE_READ_PATH=point|full`
from the #1918 differential lane. This enables the strongest available proof: run the SAME
single-SSTable fixture through `do_get` twice, forced to each arm, at a PINNED `now`, and assert
**identical rows, values, and order**. That is a CQLite-vs-CQLite differential that directly targets
gap (b) — the one gap that cannot be settled by reading the code.

Without the hatch, the bypass would silently become the only path over every single-SSTable fixture
and there would be no way to compare against the reconciler on the same bytes.

### Correctness pinning stack

| Guard | What it catches | Note |
|---|---|---|
| Flight bypass-vs-merge differential (new, via the hatch) | gap (b) liveness, static-row injection, RT/TTL divergence | the primary proof |
| `query-semantics-oracle` (core) + `flight-query-semantics-oracle` at pinned `now` | read-time reconciliation divergence a physical dump cannot see (#1742) | fixtures are single-SSTable → now exercise the BYPASS |
| `point_vs_full_differential.rs` (#1918) | point vs full read-path divergence | core-side, must stay green |
| **New ≥2-overlapping-SSTable Flight case** | that the merge arm is still taken and still correct | closes the coverage hole the bypass otherwise opens |
| Path-taken assertion (both directions) | 1 source → merger NOT entered; 2 sources → merger entered | AC #1 and #4; not a throughput assertion |
| `issue_1579_streaming_multigen_{memory,order}`, `issue_957_streaming_materializing_parity`, `issue_2096_seeking_point_merge_parity`, `step_streaming_matches_step_for_*` (`write_engine/merge/streaming.rs:1103,1149,1196,1230`) | #2988 multi-generation regression | untouched must stay untouched |

### Kill criterion (issue §4) — binding

If, after the bypass is wired and verified correct, `do_get` rows/s on the WS0 corpus does **not**
move materially, **STOP**: post the negative result (rows/s + cycles/row, warm and cold), do not
stack further levers, and re-open the attribution question — Arrow encode is the leading candidate
(WS0: 59% of cycles / 37% of throughput / 675 B/row copied, versus the 15-20% previously assumed).
"Merge CPU% fell" is explicitly NOT a pass. An unexplained result must not be built on.

## Alternatives considered (and why the recommendation beat them)

1. **B. Single-source shortcut inside `KWayMerger`** — keep the merger, but when `runs.len() == 1`
   skip `reconcile_cluster_with_overlap_counted` and set `want_cell_metadata = false`.
   *Rejected:* it removes reconciliation but leaves the whole merge machinery in the hot path — the
   per-input producer thread + bounded `sync_channel` handoff (`merge/from_readers.rs`), `MergeEntry`
   materialization, and `entry_to_row`'s reassembly + liveness pass. Its ceiling is unknown and
   strictly below the bare scan's, so a partial improvement would leave the kill criterion
   uninterpretable (is the residual gap the leftover machinery, or Arrow encode?). It is, however,
   the **lower-risk fallback** if A's gap-(b) differential cannot be made green — and it would still
   satisfy AC #2 (the per-cell HashMap) though not cleanly AC #1 (not entering `KWayMerger`).
2. **C. Route `do_get` wholesale to `Database::execute_streaming`** — reuse the bare-scan surface
   end to end. *Rejected:* it discards the entire Flight producer contract — ScanSpec token
   pushdown into the Summary-guided walk (#2412/#2413), the warm reader registry (#2310/#2356, whose
   whole point is not re-opening readers per request), Flight admission control (#2420), aggregation
   pushdown, `max_batch_bytes` (#2825), and the Arrow schema/UDT registry wiring — and would
   regress the multi-SSTable case rather than preserve it. Far more surface than the issue's scope
   fence permits.
3. **Delete `want_cell_metadata` from `CompactionPolicy::on_data_row` on read** — make the merge
   cheap instead of skipping it. *Rejected by the scope fence:* the acceptance contract §3 is
   explicit — "do not optimize `HashMap<String, CellWriteMetadata>` here — delete the path that
   builds it." It also would not satisfy AC #1 at all, and it risks the compaction writer, which
   genuinely needs the metadata.
4. **Gate the bypass on a Statistics.db-derived "no tombstones present" property** instead of on the
   source count. *Rejected:* it is the wrong predicate (the fast path handles tombstones correctly
   via `PartitionShadow`; the thing that requires a merge is >1 source), it would make the fast path
   fire unpredictably, and inferring behavior from a statistics estimate edges toward the
   no-heuristics line (#28). The source count is exact and authoritative.

## Owner decisions needed at Seam 1

1. **Scope of the bypass: warm route only, or warm + cold (`MergeInput::Paths`)?** The measured
   surface and the WS0 corpus are the warm reader route; the cold route has an equivalent
   unconditional `KWayMerger` build. Recommendation: **warm only** in this change (smallest diff on
   the highest-risk surface), cold as an immediate follow-up if the number lands.
2. **The WS0 measurement assets are NOT on this box — SETTLED at Seam 1: ratio closure on a local
   corpus.** `/home/ubuntu/ws0/ws0-corpus/rerun.sh`, `/home/ubuntu/ws0/ws0-h2h/`, and
   `/home/ubuntu/ws0/ws0-results/head-to-head-method.md` all do not exist (`/home/ubuntu/ws0` itself
   is absent; only `/home/ubuntu/workspace` is present), nothing named `ws0` appears anywhere on
   disk, the rig was never committed to the repo, and Docker/Cassandra are not installed. The box is
   a 16-vCPU Intel Xeon Platinum 8488C / 30 GB (Sapphire Rapids, c7i-family) with `perf` and
   `taskset` available, so it *can* host a measurement. CQLite's write surface is uncompressed-only
   (claim boundary #1406) while the WS0 corpus is LZ4 `chunk_length=16384`, so no locally generated
   corpus can reproduce the WS0 absolute regardless.

   **Owner decision:** generate a ~4M-row corpus locally and measure **both** the bare scan and
   Flight `do_get` over those identical bytes, same box, same pinned cores, same session, pre- and
   post-change. Pass = `do_get` closes to within ~1.3x of the same-session bare scan (from 6.0x).
   The WS0 absolute is recorded as an **owed follow-up** on a machine holding the byte-identical
   corpus, and is never restated as reproduced. This preserves the anti-#2877 discipline (acceptance
   turns on an external throughput number, not a CPU-share shift) while being explicit about what
   evidence was and was not obtained.
3. **Is the `CQLITE_FLIGHT_MERGE_PATH` escape hatch acceptable as a permanent, documented test seam**
   (as `CQLITE_READ_PATH` is), or must it be test-cfg-only? Recommendation: permanent and documented
   — it is the field's only kill switch if the bypass is ever found wrong in production.
