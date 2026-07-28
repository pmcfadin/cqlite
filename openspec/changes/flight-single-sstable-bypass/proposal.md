# Proposal: Bypass the k-way merge on a single-SSTable Flight `do_get` (issue #3058)

**Milestone:** 0.17 · **Priority:** P0 · **Routing:** design-driven (read-path reconciliation
bypass — real design latitude in WHERE the bypass lives, and a high correctness-risk surface;
no external oracle prescribes the shape) · **Issue:** #3058 · **Epic:** #2817 (0.17 throughput
program) · **Parent context:** #3023 (WS0 result) / #3026 (stock-Cassandra head-to-head).

## Why

WS0's stock-Cassandra head-to-head (#3026) measured all three surfaces on one box (c7i.4xlarge),
warm, per **physical** core, over byte-identical files (`ws0.events`, 3,999,890 rows, one `nb`
SSTable, `Data.db sha256 22d9ae224b439b2176c287a59eee6a7d1f08b4f1fafc4d2198b3da50cdce922c`):

| Surface | rows/s | cycles/row | mem B/row |
|---|--:|--:|--:|
| CQLite **bare scan** (`execute_streaming`) | **367,760** | 17,648 | 1,933 |
| CQLite **Flight** (`do_get`) | **61,151** | 96,198 | 32,188 |
| Cassandra `SELECT *` (native protocol) | 212,981 | 30,061 | 18,008 |

Flight is **6.0x slower than our own bare scan** on identical bytes and **3.48x slower than stock
Cassandra**, while the bare scan is **1.18x faster** than Cassandra. The engine is not the problem;
the shipping path is.

The cause is structural, found by profiling rather than inspection. `MergeProducer::
produce_streaming_from_readers` (`cqlite-flight/src/producer_warm.rs:44`) reaches its full-scan
branch at `producer_warm.rs:110-117` and builds `KWayMerger::new_from_readers(...)`
**unconditionally** — with **one** reader in the pruned set and nothing to reconcile. Every row then
pays `reconcile_cluster_with_overlap_counted` → `build_compaction_row_data` →
`CompactionPolicy::on_data_row` (`row_decoder/compaction.rs:585`), which passes
`want_cell_metadata = true` (`:600`), so `row_data.rs:74` allocates a
`HashMap<String, CellWriteMetadata>` **per row** and does a `column.name.clone()` + SipHash insert
**per cell**. The decoder's own comment (`row_data.rs:71-73`) says the normal read path keeps this
`None`. The resulting server CPU split is **~13% SipHash, ~16% allocator, only ~5% actual cell
decode** — and the metadata is **never consumed**: every Flight `QueryRow` sets
`cell_metadata: None` (`filter.rs:570,579`; `agg.rs:467,738,793`). It is a compaction-writer concern
leaking into the read path.

It is not gRPC/Arrow-IPC framing: Arrow encode is a separate, additive cost (59% of cycles / 37% of
throughput on this corpus) and is explicitly out of scope here.

This also invalidated an assumption #3023 was built on: WS0 compacted to a single SSTable
*specifically* to remove k-way merge from both sides of the comparison. That worked for the bare
scan; the Flight producer ran the merge anyway.

**A proven single-source fast path already exists in core** and is exactly what the 367,760 rows/s
bare scan uses: `SSTableManager::scan_stream_batched`
(`cqlite-core/src/storage/sstable/mod.rs:2312`) short-circuits at `:2321-2331` when
`readers.len() == 1`, calling `SSTableReader::scan_stream_batched`
(`reader/data_access/sequential.rs:518`). Under it, `run_scan_stream_windowed` builds its parser
with `read_shadowing = true` (`scan_stream_windowed.rs:748-751`), so SELECT-semantic reconciliation
(partition deletions, range tombstones, TTL expiry) is carried by `PartitionShadow`
(`row_decoder/partition_shadow.rs:44`) on the same `now_clock` seam. This change makes the Flight
data plane use that path when — and only when — there is exactly one source.

## What Changes

1. **A single-source fast path inside `MergeProducer`.** When the post-prune source count is exactly
   one AND the fast path's preconditions hold, the producer swaps its ROW SOURCE from
   `KWayMerger`/`StreamingMerger` to `SSTableReader::scan_stream_batched_admitted` plus a
   `(RowKey, ScanRow) → QueryRow` adapter. `MergeProducer` remains the shell, so ScanSpec predicate
   pushdown, projection, token pruning, batching/`max_batch_bytes`, cancellation, the UDT registry
   and `now_secs` stay wired at their existing sites (design.md, Option A).
2. **An authoritative, fail-closed source-count predicate.** The count is `pruned.len()` at the
   decision point in `produce_streaming_from_readers` (`producer_warm.rs:56,134`) — derived from the
   warm registry's `GenerationSet` (`warm/identity.rs:96`), itself produced by
   `probe::probe_generation_set` (`warm/probe.rs:122`) from an authoritative directory listing of
   `*-Data.db`. Never inferred from bytes or sizes (no-heuristics, #28). Anything the fast path
   cannot provably reproduce falls back to the merger.
3. **The per-cell `HashMap<String, CellWriteMetadata>` is deleted from the single-source read path**
   — not optimized. The bypass simply never enters `CompactionPolicy::on_data_row`.
4. **A forced-path escape hatch for differential testing** (`CQLITE_FLIGHT_MERGE_PATH=
   bypass|merge`), mirroring `CQLITE_READ_PATH=point|full`, so the bypass and the merge can be run
   over the SAME single-SSTable fixture and asserted row-for-row identical.
5. **Correctness pinning:** the query-semantics oracles at a PINNED `now` (core
   `query_semantics_oracle_parity.rs` / gate `query-semantics-oracle`; Flight
   `query_semantics_flight_parity.rs` / gate `flight-query-semantics-oracle`), the point-vs-full
   differential lane, a **≥2-overlapping-SSTable** Flight case (new — see Impact), and a path-taken
   assertion that fails if `KWayMerger` is entered with one source or skipped with two.
6. **A re-measurement against the external pass condition** (rows/s AND cycles/row, warm and cold),
   honoring the two measurement traps, with an explicit kill criterion.

## Non-goals

- **Not allocator work.** ~13% SipHash + ~16% allocator are *symptoms of the merge running*.
  #3028 (WS2 allocator, ≤1 alloc/row) and #3047 (hoist `RowColumnResolution`) are deliberately held
  until this lands so they are re-priced against the real remaining profile. We delete the path that
  builds the map; we do not tune the map.
- **Not Arrow encode.** 59% of cycles / 37% of throughput / 675 B/row copied on this corpus — real,
  large, and a distinct issue if pursued.
- **Not the `tokio::sync::mpsc` handoff limiter** (the unpinned-vs-pinned 18.74 s vs 11.16 s
  artefact) — distinct.
- **Not #3060** (mid-stream shutdown spin) or **#3061** (double mmap / RSS) — separately filed.
- **Not a change to the multi-source path.** With ≥2 sources the merge runs exactly as today; #2988
  (multi-generation SELECT still drives the buffered `KWayMerger`) is core-side and MUST NOT be
  regressed.
- **Not the aggregate route.** Aggregating tickets return early at `service.rs:1028` with
  `DoGetInput::Aggregate(paths)` and never reach the streaming row path (which itself early-returns
  on `self.is_aggregating()`, `producer_warm.rs:52`). Confirmed, unchanged.
- **Not the cold (`MergeInput::Paths`) route in this change** unless it falls out for free; the
  measured surface and the WS0 corpus are the warm reader route. #3068 owns cold/IO.
- **No new public API.** No CLI/Python/Node surface change.

## Impact

- **Code:** `cqlite-flight/src/producer_warm.rs` (the predicate + branch), a new adapter module for
  `(RowKey, ScanRow) → QueryRow` reusing `cqlite_core::query::build_row_from_scan_cached`
  (`select_executor/row_build.rs:227`), `producer.rs` / `producer_stream.rs` (row-source seam so the
  existing batching/cancel/`max_batch_bytes` drive loop is shared), and a small
  `cqlite-core` surface if `scan_stream_batched_admitted` needs a token-bound/ScanSpec parameter it
  does not already accept.
- **Tests:** a Flight bypass-vs-merge differential over a single-SSTable fixture; a **new
  ≥2-overlapping-SSTable Flight fixture + oracle case** (see below); path-taken assertions; extension
  of the existing Flight semantics-parity test.
- **Test-data gap this change MUST close:** every committed `test_compaction_tombstone_ttl` fixture
  dir holds exactly ONE `nb-3-big-Data.db` (verified: `rt_cross_gen`, `ttl_expired_live`,
  `shadow_row_delete`). So **both** query-semantics oracles today exercise a single-SSTable table.
  The bypass therefore gets oracle validation for free — but it would ALSO remove the only oracle
  coverage of the multi-generation merge on the Flight surface. Issue AC #4 requires a
  ≥2-overlapping-SSTable pin; this change adds one.
- **No-heuristics:** strengthened — the predicate is an authoritative count from the generation set,
  and every precondition the fast path cannot satisfy is a declared fail-closed fallback, never an
  inference.
- **Memory budget:** improves (WS0 measured 32,188 B/row on Flight vs 1,933 B/row on the bare scan);
  the <128MB streaming bound is unchanged — the fast path is the already-bounded windowed scan.
- **Doctrine:** if the bypass lands, `docs/architecture/throughput-program-2026-07.md` and #3028 /
  #3047 must be re-priced against the new profile.
