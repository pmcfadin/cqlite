# Proposal: Arrow encode on the Flight `do_get` data plane (issue #3096)

**Milestone:** 0.17 · **Priority:** P1 · **Routing:** design-driven (a perf target with real latitude
in WHICH lever is pulled and in what order; no external oracle prescribes the shape — but the
*output* is oracle-pinned) · **Issue:** #3096 · **Epic:** #2817 (0.17 throughput program) ·
**Predecessor:** #3058 (single-SSTable bypass) · **Parent context:** #3023 / #3026 / #3100 (WS0).

## Why

#3058 removed the unconditional k-way merge from the single-SSTable `do_get` path and took Flight
from 53,873 → 210,192 rows/s per physical core (3.90x), closing the bare-scan gap from 5.73x to
**1.49x**. Reconciliation on a single source is now literally zero (`reconcile_entries` 4,000,000 →
0; `cell_metadata_maps` 8,000,000 → 0), so the residual is provably not merge work:

| surface / arm | rows/s | cycles/row | IPC |
|---|--:|--:|--:|
| bare scan (`execute_streaming`) | 312,155 | **22,012** | 1.50 |
| Flight `do_get` **bypass** | 210,192 | **27,600** | 1.57 |
| Flight `do_get` merge (pre-#3058 path) | 53,873 | 122,571 | 1.49 |

**27,600 vs 22,012 cycles/row = +25%.** From the #3026/#3023 WS0 profiling, Arrow encode measured
**59% of cycles / 37% of throughput, 675 B/row copied** (payload 2.70 GB ≈ the whole uncompressed
dataset). #3058 explicitly scoped it out and named this issue as its owner; #3058's own delivered
result records the shortfall against the ~1.3x bar and hands it here. This is that work.

Code recon confirms the cost is structural, not mysterious. The encode path is:

```
service.rs:553 do_get → :764 do_get_inner → :919 spawn_streaming_from_readers
  → streaming.rs:375 spawn_streaming
  → producer_warm.rs:45 produce_streaming_from_readers → :126 bypass_reason (bypass.rs:204)
  → :137 ScanRowSource::open (bypass.rs:376) → producer_stream.rs:219 drive_row_source
  → row_source.rs:155 materialize_pending
  → cqlite-core row_build.rs:227 build_row_from_scan_cached      ← per-row HashMap (:246), into_owned (:259)
  → producer_stream.rs:351-358 (byte-cap + row-cap batching)     ← estimate_arrow_row_bytes per row
  → egress_flush.rs:83 flush_credited → producer.rs:1132 flush_buffer
  → arrow_convert.rs:197 rows_to_record_batch (schema rebuilt at :201 per batch)
  → :1296 convert_to_arrays → arrow_columnar.rs:59 transpose_columns  ← per-cell hash probe (:87)
  → builders arrow_convert.rs:1395-1917                          ← Vec<Option<T>> per column
  → streaming.rs:594 encode_do_get (FlightDataEncoderBuilder, arrow-flight 53.4.1)
```

Every row is hashed into a `HashMap<Arc<str>, Value>` once (`row_build.rs:246`), then hashed OUT of
it twice more — once by the size estimator (`arrow_size.rs:254`) and once by the transpose
(`arrow_columnar.rs:87`) — before a single Arrow buffer is touched. Then each scalar builder
materializes an intermediate `Vec<Option<T>>` before handing it to Arrow. Then the whole
`RecordBatch` is rebuilt again by arrow-flight's dictionary hydration, and re-sliced because our
4 MiB batch cap is twice the encoder's 2 MiB target.

## What Changes

1. **A committed, clean-checkout-runnable measurement rig** — because the one the issue names does
   not exist. `/home/ubuntu/ws0-local/` is absent from the delivery box (verified) and PR #3103
   shipped only `.rs` + OpenSpec, so **nothing in the issue's "Reproduction" section is
   reproducible**. This change commits: (a) a corpus generator driving the production
   `SSTableWriter` from the pinned `ws0.events` schema — 4,000,000 rows, 40,000 partitions x 100
   rows, Murmur3 token order, **uncompressed** (#1406); (b) measurement scripts implementing
   CPU-wide `perf stat -C`, verified-sibling `taskset` pinning, median-of-3, setup-subtracted
   cycles/row, warm and cold reported separately.
2. **An in-repo Arrow-buffer digest oracle** as the output-invariance check, replacing the external
   `0x0a2a390223bde6aa` which exists nowhere in this repository. It folds the emitted
   `RecordBatch` **buffers** (not `QueryRow` values), so a defect in the Arrow builders themselves
   is visible to it, and it is asserted arm-invariant (`bypass` vs `merge`) and change-invariant.
3. **A ranked set of encode levers**, landed and measured one at a time (design.md): column-major
   build straight from the scan row carrier; builder append-in-place instead of intermediate
   `Vec<Option<T>>`; fold the size estimate into the build pass; align the batch cap with the
   encoder's 2 MiB target; stop the per-batch dictionary-hydration rebuild; cache the Arrow schema.
4. **A binding kill criterion.** A correctly measured, correctly reported negative result IS a
   satisfying outcome. No lever is stacked on an unexplained result, and every lever is attributed
   individually against the same-session baseline.

## Non-goals

- **Not the `tokio::sync::mpsc` handoff limiter** (unpinned 18.74 s vs 11.16 s pinned, ~1.98M vs
  310K voluntary context switches) — distinct, already fenced out by #3058.
- **Not the allocator or `RowColumnResolution` work** (#3028, #3047). They are held for re-pricing
  against the post-#3058 profile; lever 1 below overlaps their territory and its measured result
  should re-price them, but this change does not absorb them.
- **Not the cold / `MergeInput::Paths` route** (#3068), and not the merge arm's encode path beyond
  keeping it byte-identical.
- **Not a change to query results.** The digest, the forced-path differential, the wire-frame golden
  and the semantics oracles all stay green; anything that moves them is out of scope by definition.
- **Not a Cassandra head-to-head.** No Cassandra on the delivery box; the committed corpus is
  CQLite-written and uncompressed, so the WS0 absolutes and the stock-Cassandra leg stay **owed**.
- **No new public API.** No CLI / Python / Node surface change. The Flight wire format is unchanged.

## Impact

- **Code:** `cqlite-core/src/export/arrow_convert.rs`, `arrow_columnar.rs`, `arrow_size.rs`;
  `cqlite-core/src/query/select_executor/row_build.rs` (a column-major emit seam alongside the
  existing `QueryRow` one — the bare scan's own adapter, so both surfaces benefit);
  `cqlite-flight/src/producer.rs`, `producer_stream.rs`, `egress_flush.rs`, `streaming.rs`,
  `batch_bytes.rs`.
- **New committed assets:** a corpus generator (natural home: `tools/*`, already a workspace member
  glob and the precedent `tools/flight-loadgen` sets), measurement scripts, and a method doc under
  `docs/reports/`.
- **File-size campsite rule bites immediately:** `arrow_convert.rs` is **2,596 lines** (~4x the
  ~800 target) and `producer.rs` is **3,243**. Any lever that grows them must instead split by
  responsibility (epic #1116). This is a real cost of the change and is planned, not waived.
- **No-heuristics:** unaffected in principle — the column set and types come from the ticket schema;
  no lever may infer a type or a width from byte content. Lever 1 removes a name→value hash lookup,
  which makes column identity *more* explicit (positional, from the authoritative schema), not less.
- **Memory budget:** improves or holds. Levers 2 and 3 remove per-batch intermediate vectors; lever
  4 halves the peak framing working set. The <128MB streaming bound and the egress credit
  reservation (`worst_case_batch_capacity_bytes`) are unchanged contracts.
- **Correctness risk concentration:** entirely in lever 1 (a second row-emit shape) and lever 7
  (borrowed text/blob). Both are gated behind the digest oracle + the #3058 forced-path
  differential; lever 7 is explicitly a stretch and may be dropped without failing the change.
- **Doctrine:** if a lever lands, `docs/architecture/throughput-program-2026-07.md` and the #3028 /
  #3047 estimates must be re-priced against the new profile.
