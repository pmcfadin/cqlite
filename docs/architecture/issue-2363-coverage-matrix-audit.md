# Issue #2363 — Coverage-matrix blind-spot audit (field axes vs local E2E coverage)

Status: COMPLETE 2026-07-13 — six axes mapped, holes verified against code routing, issues #2370–#2381 filed.
Method: issue #2363 body (owner-approved 2026-07-12). Round-7 context: #2286.
Do-not-refile set: #2358 (E2E not on PRs), #2362 (uncompressed scan path + streaming asserts),
#2356 (per-query-snapshot mode), missing-index-components synthesis (noted on #2295),
broad-lanes push-only.

## Axis: SSTable format — BTI (`da`) through Flight/Trino  [mapper: COMPLETE]

### Verdict summary

| Cell | Verdict |
|---|---|
| BTI core-level read (point + scan) | COVERED (baseline) |
| BTI via Flight do_get — point lookup | **UNCOVERED** (E2E); only `nb-big` fixtures reach do_get |
| BTI via Flight do_get — full scan | **UNCOVERED** (E2E); BTI scan tested only at core |
| BTI through Trino connector / testbed | **UNCOVERED**; `cassandra:5.0` default writes BIG, testbed DDL has no BTI option (self-documents `nb-1-big`) |
| Flight footprint accounting for BTI sidecars | PARTIAL (unit-level only, `warm/budget.rs`; no data read) |

### Evidence

**BTI corpora exist**: `test-data/datasets/sstables/test_da/` (`simple_table`, `collection_table`,
`wide_table`, `ttl_table`; `da-2-bti-*` components incl. `Partitions.db`/`Rows.db`, no
Summary.db/Index.db) + `test-data/datasets/corruption/test_comp_corrupt/` BTI corruption fixtures.

**Core baseline COVERED**: dedicated suites in `cqlite-core/tests/` — e2e read
(`issue_660_bti_end_to_end_read.rs`, `issue_657_da_foundation.rs`), point lookup
(`issue_831_bti_reader_point_lookup.rs`, `issue_755`, `issue_1650`, `issue_1968`), traversal/scan
(`issue_832_bti_traversal.rs`, `issue_1577_bti_scan_stream_prefix_parity.rs`, `issue_1574`,
`issue_1647`), parity/roundtrip (`issue_909`, `issue_911_bti_sstabledump_parity.rs`, `issue_1103`,
`test_issue_1649_bti_dfs_zero_clone.rs`).

**Flight E2E routes BIG only** (routing evidence):
- `cqlite-flight/tests/do_get_transport_test.rs:312-314` — `.join("nb-1-big-Data.db")` (test_basic.simple_table)
- `cqlite-flight/tests/point_read_corpus_parity_test.rs:197-204,390` — `nb-1-big-Data.db` + CompressionInfo; header: "REAL, compressed, `nb`-format corpus tables"
- `cqlite-flight/tests/collection_collapse_parity_test.rs:255-257` — `nb-1-big-Data.db`
- Only BTI reference in cqlite-flight: `src/warm/budget.rs:132-146`
  `footprint_includes_bti_partitions_and_rows_sidecars` — empty stub files, LRU byte math only,
  no read/parse/scan/do_get.
- Structural signal: flight boundary-key resolution uses `SummaryReader::open`
  (`src/producer.rs:296-309`) — Summary.db is BIG-only; BTI would depend on a fallback never
  exercised via Flight tests.

**Trino/testbed writes BIG only**:
- `trino-connector/docker/docker-compose.yml:26` — `image: cassandra:5.0`, no `sstable_format` override anywhere in `docker/`
- `trino-connector/docker/field-repro-data.cql:7` — comment "ONE flush -> ONE `nb-1-big` SSTable, LZ4"
- No occurrence of `sstable_format` / `bti` / `da-*-bti` anywhere under `trino-connector/` or `easy-db-lab-kits/` templates; loadgen uses default format.

### Candidate hole (to verify/file)
**HOLE-BTI-1**: BTI (`da`) tables have zero E2E coverage through Flight do_get (point + scan) and
the Trino testbed. Concrete fix: (a) Flight integration tests pinned to `test_da` corpus tables
(point lookup + full scan + LIMIT), (b) testbed variant provisioning Cassandra with
`sstable_format: bti` (or per-table option) so field-shape E2E exercises `da`.
Note: the `SummaryReader` boundary-key path (producer.rs) may be functionally broken or
fallback-dependent for BTI — the E2E test may be RED on first run; that is the point.

## Axis: Concurrency — N concurrent do_gets (field runs 8)  [mapper: COMPLETE]

### Verdict summary: **UNCOVERED** (automated)

No test issues ≥2 CONCURRENT do_gets (or concurrent Trino queries) against one server with an
assertion able to observe a concurrency failure. The only real N-concurrency is a non-asserting
field tool; the only asserted concurrency is core-layer, not the Flight path.

| Cell | Evidence | Verdict |
|---|---|---|
| N-concurrent Trino/do_get load | `easy-db-lab-kits/trino-loadtest/driver.py` — real `threading.Thread × N` workers (`--threads 8/16/50`), BUT `main()` unconditionally `return 0`; prints qps/p99/errors, asserts nothing | PARTIAL (human-read tool, not a test) |
| Flight integration tests (`cqlite-flight/tests/do_get_transport_test.rs`) | every test drives ONE client stream (`tokio::spawn` is the server); `await_in_flight_settled` observability is single-stream | UNCOVERED for the axis |
| Phase-active gauge #2361 (`src/obs.rs` units) | `phase_active_counter_reflects_concurrent_overlap_not_a_flag` builds 2 synchronous guards on ONE thread; never N real do_get RPCs | PARTIAL (arithmetic only) |
| Thread collapse #2316 (`cqlite-core/tests/issue_2316_merge_thread_budget.rs`) | bounds ONE merge's internal M=4 fan-out via `/proc/self/task` peak; header names concurrent do_gets as motivation but never runs them | UNCOVERED for aggregate load |
| Core golden-path concurrent scans (`tests/golden_path_*`) | real `tokio::spawn × 5-10` but hits `SSTableReader` directly (not do_get), over COMPRESSED fixtures | wrong layer |
| Concurrency × uncompressed (non-stitching) path | no test combines them; #2362's streaming asserts are single-stream | UNCOVERED |

### Candidate holes (verify/file)
- **HOLE-CONC-1**: no asserting N-concurrent-do_get test against one Flight server — the exact gap
  #2316 and #2361 fell through. Fix: integration test spawning N≥8 simultaneous do_get streams
  (mixed scan/point/LIMIT), asserting completion within timeout + `in_flight`/`phase.active`
  settling to baseline + bounded process thread count.
- **HOLE-CONC-2**: `cqlite.rpc.phase.active` never validated through real overlapping do_gets
  (mid-flight ==N assertion); a set-flag regression via the real handler path would pass today.
- **HOLE-CONC-3**: process thread budget under ≥2 simultaneous merges/do_gets untested (the actual
  #2316 collapse condition).
- **HOLE-CONC-4**: (subsumes 1) concurrency × uncompressed scan path specifically.
- Consider also: promote `trino-loadtest/driver.py` to gate-able (exit nonzero on error-rate/p99
  SLA breach) so field-shaped load can assert.

## Axis: Read mode + failure injection + environment  [mapper: COMPLETE]

| Cell | Covering test | Verdict |
|---|---|---|
| Live-dir server read | `cqlite-flight/tests/do_get_transport_test.rs :: do_get_over_transport_reads_live_set_not_snapshot` (E2E over tonic) | COVERED |
| Per-query-snapshot server read | ticket-JSON wiring only (`ReadModeWiringTest.java`) | UNCOVERED at server level (tracked #2356, do-not-refile) |
| Live memtable-invisible contract | `trino-connector/docker/e2e-test.sh :: "unflushed row invisible (live mode)"` | COVERED (docker E2E) |
| clearSnapshot mid-query (#2352/PR #2357) | `cqlite-flight/src/warm/registry_tests.rs :: warm_hit_after_snapshot_teardown_rebuilds_instead_of_enoent` — real `remove_dir_all` but BETWEEN warm calls; scan surrogate `decode_names`, not the do_get RPC | PARTIAL — **fix has no Flight-transport E2E**; regression was "9 nightly Flight↔Trino E2E failures" yet coverage is registry-unit |
| Teardown inside the stat-gate window [T, T+δ) while streaming | `registry.rs:655-661` documents the accepted residual verbatim; no test injects it | UNCOVERED — tracked #2356 (do-not-refile) |
| Missing components at snapshot time | `cqlite-core/tests/issue_2295_snapshot_component_completeness.rs` (staged partial set, core-only) | PARTIAL (synthesis gap = #2295 note, do-not-refile); no Flight-surface exercise of component-incomplete snapshot |
| Pod kill / failover mid-stream (#2241) | `CqliteFlightPageSourceFailoverTest.java` etc. — FakeStream throwing gRPC UNAVAILABLE (incl. mid-stream-after-batch no-double-rows guard) | PARTIAL — simulated exception only; repo-wide NO real process/pod kill mid-query |
| JVM/classpath alignment (#2300) | `ArrowJavaVersionPinTest.java` — asserts resolved jar versions (arrow 19.0.0, netty), sees the NettyAllocationManager class-load failure | COVERED |
| Docker vs bare-metal | both E2E lanes docker-compose-bound | PARTIAL (docker only) |
| refresh()/generation turnover DURING open scan | `issue_1749_sstable_freshness_refresh.rs` + `registry_tests.rs` eviction/swap tests — all turnover observed on the NEXT call, never during an in-flight stream | PARTIAL |

### Candidate holes (verify/file)
- **HOLE-FI-1**: #2352 fix regression coverage never exercises the real Flight do_get surface —
  add a transport-level E2E: open server, warm, clearSnapshot the backing dir, re-query over gRPC,
  assert rebuilt (no ENOENT, no hang w/ timeout).
- **HOLE-FI-2**: no real process-kill/failover injection (docker `kill` of a flight pod mid-stream
  in the testbed; #2241 shipped simulated-exception units only).
- **HOLE-FI-3**: generation turnover (compaction add+remove) DURING an in-flight streaming scan —
  all existing tests flip generations between calls.
- (Environment bare-metal lane: candidate NEEDS-OWNER, likely wont-fix/accepted.)

## Axis: Data shape — tombstone/TTL/shadowed via Flight  [mapper: COMPLETE]

Layer decomposition drives every verdict: **L1** = producer-level (in-process `MergeProducer`,
no gRPC, `cqlite-flight/src/*_tests.rs`); **L2** = real gRPC transport
(`tests/do_get_transport_test.rs`); **B** = Trino connector / field-repro E2E.

| Cell | Verdict | Evidence |
|---|---|---|
| Row-tombstone shadowing, L1 | COVERED | `streaming_tests.rs:585 stream_limit_over_shadowed_data_returns_k_surviving_rows` (#2361 pin); `point_read_tests.rs:292` tombstoned corpus |
| Overwrite (LWW), L1 | COVERED | `point_read_tests.rs:338` asserts reconciled row |
| **TTL expiry via Flight — any layer** | **UNCOVERED** | no TTL param populated anywhere in flight testutil (`testutil.rs:54-158` pass `None`); no `CQLITE_TTL_NOW_OVERRIDE` seam in flight |
| **Range-tombstone via Flight — any layer** | **UNCOVERED** | no `RangeTombstone`/`delete_range` in cqlite-flight; only `DeleteRow` helper |
| **Shadowing over REAL transport (L2)** | **UNCOVERED** | `do_get_transport_test.rs:342` multi-SSTable fixture = distinct interleaved keys, no deletes/overwrites/TTL — partitions == surviving rows, the exact condition that hid #2361 |
| Wide partitions, L1 | PARTIAL | 5-clustering-row "wide" (`point_read_tests.rs:446`); real `test_wide_rows` corpus producer-level only |
| Partition-count scale, L2 | PARTIAL | transport fixtures = 20 rows |
| Query-semantics oracle routing (#1742) | **CORE-ONLY** | `query_semantics_oracle_parity.rs` imports cqlite_core Database; zero flight/do_get/grpc hits; oracle cases (`shadow_row_delete`, `ttl_expired_live`, `rt_cross_gen`) are the repo's ONLY range-tombstone+TTL reconciliation coverage and never touch Flight |
| Shadowed-LIMIT assertion quality, L1 | PARTIAL | the #2361 pin asserts `total == LIMIT` — COUNT only, not WHICH surviving rows; set-membership check exists only on the non-shadowed transport test |
| Shadowed data in Trino/E2E (B) | **UNCOVERED** | `field-repro-load.sh`: 3-row keyvalue + ≥100k `loadtest.keyvalue` explicitly "two batches can never collide" — no DELETE/UPDATE/TTL anywhere in E2E |
| Wide-partition E2E (B) | PARTIAL | 100k partitions but each a single narrow row |
| E2E assertion quality (B) | PARTIAL | #2193 check asserts exact row set on tiny table; #2264 check on 100k table is count-only |

### Candidate holes (verify/file)
- **HOLE-SHAD-1**: shadowed data (tombstone/overwrite) never crosses the real do_get gRPC transport —
  add L2 fixture with cross-generation deletes/overwrites; assert reconciled ROW SET, not count.
- **HOLE-SHAD-2**: TTL-expired data unexercised anywhere in the Flight crate (no TTL seam at all).
- **HOLE-SHAD-3**: range-tombstones unexercised anywhere in the Flight crate.
- **HOLE-SHAD-4**: query-semantics oracle (#1742 reconciliation guard) is core-only — route the
  canonical oracle SELECTs through Flight do_get (and ideally Trino) as a parity lane.
- **HOLE-SHAD-5**: field-repro E2E structurally cannot reproduce #2361-class bugs (partitions ==
  surviving rows at scale; count-only assert on the 100k table) — add shadowed rows + set-membership
  (or shadowed-count-delta) assertions to the load script.
- **HOLE-SHAD-6** (minor): upgrade the L1 #2361 pin from count-only to surviving-row-set assertion.

## Axis: Format × compression scan-path routing  [mapper: COMPLETE]

### Routing branches (the ground truth the matrix is judged against)
- **Branch A** `stream_all_partitions_for_compaction` (`cqlite-core/.../data_access/compaction.rs:559-596`):
  `!requires_chunk_stitching()` → non-stitching streaming; else sliding-window chunk-stitch.
- `requires_chunk_stitching()` (`data_access/mod.rs:165-169`) = `V5CompressedLegacy && is_nb_format()`:
  TRUE = BIG nb COMPRESSED (stitch); FALSE = BIG nb UNCOMPRESSED (`V5_0Uncompressed`) AND BTI da.
- **Branch B** `stream_all_partitions_cancellable` (`full_index_stream.rs:280-320`): BIG w/ Index.db →
  full-index walk (`Streamed | FellBack`); BTI or FellBack → materialising `sequential_scan`.
- **Branch C** inner split (`full_index_stream.rs:219-226`): `compression_info` Some → compressed
  offset-window arm; None → `read_uncompressed_verified` (**the #2361 arm**). The COMPRESSED arm is
  structurally unreachable for the nb/da corpus (compressed nb → stitch; BTI never enters Branch B)
  — defensive/dead, no scan test drives it.
- Writer fact: `WriteEngine` emits UNCOMPRESSED BIG only (`export.rs:794`) — every writer-produced
  fixture routes Branch A FALSE → Branch C uncompressed arm. Any test claiming stitch coverage from a
  writer fixture would be vacuous (the #2362 lesson; `issue_1578` documents + avoids the trap correctly).

### Matrix (Core scan / Flight do_get)

| Cell | Core | Flight | Verdict |
|---|---|---|---|
| BIG nb × LZ4 / Snappy / Zstd / Deflate | COVERED (`issue_1082` full-scan vs goldens over `test_comp/*`; `issue_1104` stitch stream) — fixture-presence-gated (SKIP when `test_comp` binaries absent) | **UNCOVERED** — no compressed fixture ever reaches the Flight producer | PARTIAL |
| BIG nb × UNCOMPRESSED (#2361 arm) | COVERED (`full_index_stream_tests.rs` asserts Streamed branch; `issue_592`, `reader_compression_tests`) | COVERED (`do_get_transport_test.rs:176` + `streaming_tests` writer fixtures — all uncompressed by construction) | COVERED |
| BTI da × LZ4 | COVERED (`issue_1580_scan_token_order_oracle.rs:140` + BTI suites over `test_da`) | UNCOVERED | PARTIAL |
| BTI da × Snappy / Zstd / Deflate / UNCOMPRESSED | **UNCOVERED — zero fixtures exist** (whole BTI compression axis rests on one LZ4 corpus) | UNCOVERED | UNCOVERED |

### Field-testbed inversion (key finding)
The LIVE testbed (cassandra-easy-stress, no compression override) writes **BIG nb × LZ4 → the
STITCH path** through Flight; the IN-REPO Flight fixtures (WriteEngine) are all **UNCOMPRESSED →
the #2361 arm**. So the two E2E surfaces cover **opposite branches with no overlap**: the live kit
cannot reproduce #2361-class bugs, and the in-repo Flight tests have no net under the stitch path
the field actually runs.

### Candidate holes (verify/file)
- **HOLE-FMT-1**: compressed (stitching) BIG-nb corpus fixtures through Flight do_get — the live
  testbed's actual path has no in-repo E2E net.
- **HOLE-FMT-2**: BTI compression-axis fixtures missing (Snappy/Zstd/Deflate/uncompressed-BTI) —
  fixture commissioning; constrained by corpus-regen freeze (#2222) / fixture epic #2303.
- **HOLE-FMT-3** (adjudication, P3): Branch C compressed offset-window arm is dead-defensive for
  the supported corpus — document or add a reachability note/test.
## Axis: Query shape through do_get  [mapper: COMPLETE]

Layers: **rpc** = real do_get handler/tonic transport (`cqlite-flight/tests/*.rs`);
**seam** = streaming seam called directly (`src/streaming_tests.rs` via `stream_batches_raw`/
`spawn_streaming` — bypasses `do_get_setup` routing); **TRINO** = Java connector units;
**E2E** = loadtest/testbed. Routing anchor: `service.rs do_get_setup` L625-680
(`is_aggregating() → Aggregate` else warm `Rows`; point-vs-scan via `point_read::detect_route`).

| Query shape | rpc | seam | TRINO | E2E |
|---|---|---|---|---|
| 1 point lookup (PK eq) | COVERED (result + `access_path=streaming_partition_lookup` attribution; multi-key/dedup variants; compressed corpus dual-path parity) | n/a | PARTIAL (pushdown/ticket only) | **UNCOVERED** (no PK-eq query in `driver.py` default set or test plan) |
| 2 LIMIT-k | COVERED result-only (`do_get_over_transport_enforces_limit`: exactly 7 over 2 flushes, kills single-gen early-stop) | COVERED (+ scan-progress remainder flush) | PARTIAL (ApplyLimit pushdown gate) | PARTIAL (LIMIT 100/1000 under load; qps/p99 printed, nothing asserted) |
| 3 predicate+LIMIT sparse (#2157/#2361 class) | **UNCOVERED** (no rpc test combines them; predicate-only and limit-only exist separately) | COVERED result-only (`stream_limit_returns_k_matches_even_when_concentrated_past_limit_index`, shadowed-LIMIT pin) | **UNCOVERED** | **UNCOVERED** |
| 4 full scan | COVERED — richest: `metrics_capture_test.rs` asserts `phase.duration merge_setup` (#2157 stall localizer), closed phase set, incremental `rpc.rows`, `access_path=full_scan`; warm-hit zero-reader-opens | COVERED — the repo's ONLY bounded-memory/TTFB asserts (`slow_consumer_bounds_produced_batches`, `first_batch_available_before_merge_completes`) | PARTIAL (FakeStream drain, result-only) | PARTIAL |
| 5 count(*)/aggregate | **UNCOVERED** (no aggregation ticket ever sent through do_get/transport; the `Aggregate` branch of `do_get_setup` unexercised at rpc layer) | PARTIAL (`build_aggregate_response` content-only) | COVERED result-only (per-token-range tickets, merger, failover — all FakeStream) | PARTIAL (count(*) vs CQL exact-match is a MANUAL checklist step) |
| 6 midstream cancel (#2361 teardown) | COVERED (`do_get_client_drop_midstream_releases_producer_under_backpressure` + LIMIT variant — `rpc.in_flight` settles to baseline = real teardown assert) | COVERED (drop-cancels-merge, emitted-prefix attribution, #2264 unpark, panic→terminal error) | **UNCOVERED** (no `CqliteFlightPageSource.close()`/cancel teardown test) | PARTIAL (monitor/triage only) |

### Cross-cutting blind spot
**`cqlite.rpc.phase.active` (#2361 hardening) has ZERO rpc-level read-back assertion** — registered
in catalog, up/down semantics unit-tested in `obs.rs` only; `metrics_capture_test.rs` reads
`phase.duration` but never `phase.active`. No test observes it climbing during a real do_get or
settling on real cancel. (Converges with HOLE-CONC-2.)

### Observability-assertion distribution
Bounded-memory/TTFB: only full-scan, only seam. Phase timing: only full-scan rpc. Teardown
(`rpc.in_flight`): only cancel cell. Everything else (LIMIT, predicate, aggregate, all TRINO,
all E2E): result/count-only.

### Candidate holes (verify/file)
- **HOLE-QS-1**: predicate+LIMIT sparse never reaches the rpc layer (nor Trino/E2E) — the #2157
  class is pinned only at the seam. Add a transport test (sparse predicate + LIMIT over multi-gen).
- **HOLE-QS-2**: aggregate/count(*) tickets never exercised through real do_get — the
  `DoGetInput::Aggregate` branch of `do_get_setup` has no rpc-layer test.
- **HOLE-QS-3**: `phase.active` end-to-end read-back (full scan climbing; cancel settling)
  — merge with HOLE-CONC-2.
- **HOLE-QS-4**: Trino-side cancel/close teardown of the Flight stream untested.
- **HOLE-QS-5**: E2E has no point-lookup query shape at all (field runs presumably include them).
- (LIMIT bounded-memory at rpc layer: adjacent to #2362's streaming asserts — check overlap
  before filing; #2362 covers the uncompressed path's streaming assertions specifically.)

## Synthesis — confirmed holes and filed issues

### Spot-verification (lead, 2026-07-13)
Independent greps confirmed the load-bearing claims: no `test_da`/BTI data read anywhere in
cqlite-flight (only a comment + the budget unit test); no `join_all`/`JoinSet`/multi-handle
concurrency in `cqlite-flight/tests/`; no TTL/range-tombstone seam in flight testutil or tests;
no aggregate ticket in `cqlite-flight/tests/`; no `phase.active` read-back outside `obs.rs` units;
`driver.py main()` unconditionally `return 0`.

### The headline pattern
Coverage is an **inverted pyramid**: rich at the core/producer/seam layers, thinning to near-zero
at exactly the layers the field exercises — the real gRPC transport, N-concurrency, and the
testbed. And the two E2E surfaces cover **opposite compression branches with no overlap**
(in-repo Flight = uncompressed/#2361 arm only; live testbed = LZ4 stitch only). Every recent
field-only escape (#2316, #2352's E2E blindness, #2361, #2264) maps to a cell this audit marks
UNCOVERED or PARTIAL.

### Confirmed holes → filed issues (one per hole group, concrete fix in each)

| Issue | Holes | Priority |
|---|---|---|
| **#2370** — Concurrent do_get integration coverage (N≥8 streams; in_flight/phase.active settle + mid-flight read-back; process thread budget; uncompressed variant) | CONC-1..4, QS-3 | P1 |
| **#2371** — Shadowed data through the real do_get transport (row-set asserts; TTL + range-tombstone seams; sparse predicate+LIMIT at rpc; upgrade seam pin count→set) | SHAD-1,2,3,6 + QS-1 | P2 |
| **#2372** — BTI (da) E2E via Flight do_get + testbed BTI variant (SummaryReader boundary-key risk — may be red on first run) | BTI-1 | P2 |
| **#2373** — Compressed (stitching) BIG-nb through Flight do_get (close the testbed-inversion gap) | FMT-1 | P2 |
| **#2374** — Query-semantics oracle routed through Flight do_get (parity lane) | SHAD-4 | P2 |
| **#2375** — Aggregate/count(*) through real do_get (`DoGetInput::Aggregate` rpc-layer test) | QS-2 | P2 |
| **#2376** — #2352 clearSnapshot regression at Flight transport + generation turnover during in-flight scan | FI-1, FI-3 | P2 |
| **#2377** — Testbed/loadtest E2E gaps: shadowed rows in load table, point-lookup + sparse-predicate+LIMIT queries, gating driver exit, automated count(*) parity | SHAD-5, QS-5, CONC driver note | P2 |
| **#2378** — Trino page-source cancel/close teardown test | QS-4 | P3 |
| **#2379** — Real process-kill mid-stream injection in testbed | FI-2 | P3 |
| **#2380** — BTI compression-axis fixtures (Snappy/Zstd/Deflate/uncompressed-BTI) — under fixture epic #2303; constrained by corpus-regen freeze #2222 | FMT-2 | P3 |
| **#2381** — Adjudicate Branch C dead compressed offset-window arm | FMT-3 | P3 |

Filed 2026-07-13: #2370–#2381.

### Suspected-but-unconfirmed (recorded, NOT filed)
- Flight boundary-key resolution via `SummaryReader` (producer.rs:296-309) may be broken or
  fallback-dependent for BTI — will be adjudicated by the BTI E2E issue's first red run.
- Wide-partition (many-rows-per-partition) representativeness at the transport layer — fixtures are
  small everywhere; covered indirectly by several filed issues rather than its own.

### NEEDS-OWNER
- Bare-metal environment lane: both E2E lanes are docker-compose-bound. Accept docker-only as the
  supported posture, or commission a bare-metal lane?
- Sequencing: the P1 concurrency issue vs the already-queued #2362 (uncompressed testbed scenario)
  and #2366 (O(partitions) cliff) — they touch the same surfaces.
- Do-not-refile set honored throughout: #2358, #2362, #2356, #2295 note, broad-lanes push-only.
