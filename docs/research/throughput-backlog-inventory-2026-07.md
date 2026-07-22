# Throughput backlog inventory — 2026-07-21

Purpose: a complete dedup map of existing GitHub issues touching scan-path / Flight / connector
performance, so 0.17 throughput-program filings extend or re-milestone existing work instead of
duplicating it. Read-only survey — no GitHub writes were made compiling this.

Scope note: epics **A–M** (read-path perf audit + parser perf audit, filed 2026-07) are **entirely
CLOSED** — every epic issue and the overwhelming majority of children shipped. They are listed here
for dedup completeness (a 0.17 filer must know this ground was already covered) and because a
handful of children are still open. Epics **AA/AB/AD/AE/AF** (export-path perf audit) are **mixed**:
epics open, several children open, several closed, milestone 0.16.

---

## 1. Epic AB #1467 (streaming egress) + children

| # | Title | State | Milestone | Priority | Claims / notes |
|---|---|---|---|---|---|
| 1467 | Epic AB — Streaming egress: eliminate materialize-then-emit | OPEN | 0.16 | P1, epic | Umbrella: make every egress path look like `cqlite-cli/src/commands/export.rs` (true streaming). Source: `docs/reports/export-path-performance-audit-2026-07-01.md` (AB1–AB9). |
| 1476 | AB1 — Flight `do_get`: stream batches instead of materializing whole table | CLOSED | 0.16 | P1 | Full Flight streaming rewrite; AA3 rides it. Shipped. |
| 1477 | AB2 — Flight: LIMIT pushdown on the ticket | CLOSED | 0.16 | P1 | Interim memory bound; no longer a prerequisite for AB1. |
| 1478 | AB3 — CLI one-shot `-e SELECT`: stream json/csv/parquet instead of materializing | OPEN | 0.16 | P1 | Still open — one-shot CLI path still slurps. |
| 1479 | AB4 — `read-sstable`/`export-sstable`/`read`: stop slurping via `get_all_entries()` | OPEN | 0.16 | P2 | |
| 1480 | AB5 — `import`: stream JSON, single-pass CSV, skip-with-count malformed rows | OPEN | 0.16 | P2 | |
| 1481 | AB6 — `--out table`: bound + document buffering-by-design path | OPEN | 0.16 | P3 | |
| 1482 | AB7 — batch `ParquetWriter::write`: cap row-group size | OPEN | 0.16 | P2 | Stop one-group/whole-file. |
| 1483 | AB8 — CLI: move decorative chatter off stdout | CLOSED | 0.14 | P0, bug | Piped-output corruption fix. Shipped. |
| 1484 | AB9 — delta-scan: use the #1143 windowed driver instead of stitching whole Data.db | OPEN | 0.16 | P2 | Cross-links read-path epics A–G. |

## Epic AE #1470 (per-cell conversion cost) + children

| # | Title | State | Milestone | Priority | Claims / notes |
|---|---|---|---|---|---|
| 1470 | Epic AE — Per-cell conversion cost (export-side parser J1 instance) | OPEN | 0.16 | P2, epic | Resolve per-column accessor once instead of per-cell string-hash dispatch. Source: same audit doc (AE1–AE6). |
| 1495 | AE1 — arrow_convert: resolve per-column accessor once | CLOSED | 0.14 | P2 | Kills per-cell HashMap lookup. Shipped. |
| 1496 | AE2 — capacity-hinted builders, drop per-value clones + double-Vec blob | CLOSED | 0.14 | P2 | Shipped. |
| 1497 | AE3 — delta emit: hoist schema-constant sets + index cell dispatch | OPEN | 0.16 | P2 | Stop O(R·V·cells). |
| 1498 | AE4 — delta emit: intern column names | OPEN | 0.16 | P3 | Stop per-cell `ColumnId` String clone. |
| 1499 | AE5 — CLI json/csv: borrow keys, fix `== "null"` sentinel bug | CLOSED | 0.14 | P0, bug | Shipped. |
| 1500 | AE6 — Node `exportParquet`: `spawn_blocking` | OPEN | 0.16 | P2 | Unblock event loop; ~5-word from memory list. |

**Sibling export-audit epics (same source doc, found during traversal — not separately requested but load-bearing context):**

| # | Title | State | Milestone | Priority |
|---|---|---|---|---|
| 1466 | Epic AA — Flight connector hardening (ticket safety, cancellation, lifecycle, tests) | OPEN | none | P0, epic |
| 1472 | AA2 — Flight: cap ticket size in-crate (defense-in-depth) | OPEN | 0.16 | P3 |
| 1469 | Epic AD — Round-trip parity + determinism + perf gate | OPEN | none | P1, epic |
| 1490–1494 | AD1–AD5 (parity/determinism/bench fixtures) | CLOSED | — | — |
| 1471 | Epic AF — Dead code, feature hygiene, campsite | OPEN | none | P2, epic |
| 1502–1505 | AF1–AF4 (dead-code delete, feature split, file splits) | OPEN | 0.16 | P2–P3 |
(AA1/AA3/AA4/AA5 = #1430/#1473/#1474/#1475, all CLOSED.)

---

## 2. Read-path perf audit (Epics A–G) and parser perf audit (Epics H–M)

**All 13 epics (A,B,C,D,F,G,H,I,J,K,L,M — note: no separate "E" epic number gap, E = #1517 "Hot-path mechanics") are CLOSED.** Nearly every child is CLOSED. This entire audit program (2026-07) shipped.

| Epic | # | State | Title |
|---|---|---|---|
| A | 1513 | CLOSED | Measurement first: read-path benchmark + regression-gate suite |
| B | 1514 | CLOSED | Ship the read cache (B1–B5: shared chunk cache, dead-cache delete, decompressor cache, key→offset cache, observability) |
| C | 1515 | CLOSED | Point-lookup fast path (C1–C5: digest-first index lookup, ReadAt positional reads, zero-copy BTI walk, hoisted rehash, range short-circuit) |
| D | 1516 | CLOSED | Streaming by default: pushdown + bounded memory (D1–D6: LIMIT/OFFSET pushdown, streaming aggregates, streaming merge, oracle-ordered merge, BulletproofReader retirement, byte-bounded result budget) |
| E | 1517 | CLOSED | Hot-path mechanics: allocations, copies, syscalls |
| F | 1518 | CLOSED | Concurrency & scheduling: even latency under load (F1–F6: reader-map RwLock, streaming-channel batching, blocking I/O off async, admission control, lock hygiene, hardware sympathy) |
| G | 1519 | CLOSED | Reader consolidation (G1–G4: delete dead reader stacks, one ChunkSource decode plane, one PartitionLocator, confine TombstoneMerger) |
| H | 1601 | CLOSED | Parser measurement + adversarial safety net (fuzz, benches, work-counters) |
| I | 1602 | CLOSED | Parser correctness landmines (VInt signedness, swallowed stats, Blob fallback, BTI cap) |
| J | 1603 | CLOSED | One decoder: per-column dispatch + parser stack consolidation |
| K | 1604 | CLOSED | Row/cell hot-loop mechanics (per-row constant factors; K1 PartitionDriver, K4 Arc-identity clone kill, K5 zero-copy Value, K6 hardware-sympathy bundle) |
| L | 1605 | CLOSED | BTI parser: floor-walks and zero-alloc descent |
| M | 1606 | CLOSED | Parser metadata honesty + structural hygiene |

**Open stragglers from these epics (dedup-relevant, still live):**

| # | Title | State | Milestone |
|---|---|---|---|
| 2177 | Nit follow-up (#1598 G2): stale `range()` reference in scan_stream_windowed.rs comment | OPEN | none |
| 2165 | G2 follow-up: route iterate_all_partitions/sequential_scan decode through ChunkSource | OPEN | none |
| 1655 | M2 — Campsite splits on parser files epics I–L touch (tracking) | OPEN | none |
| 1883 | Rust-side per-row alloc budget (dhat/allocator hook), ratchets #1447/#1445/#1446 | OPEN | 0.17 (already there) |
| 1818 | B-series follow-up: BIG point-read cache site dead on public path | CLOSED | — |
| 2059 | Global bounded key→partition-offset cache (Cassandra key-cache model) | CLOSED | — shipped, gated 2565 nits open |
| 2565 | Nits follow-up: global bounded key cache (#2059/PR #2554) — doc & test-hygiene | OPEN | none |
| 2561 | BTI point-read: chunk-straddling partition decode trusts closure-fired-as-complete → whole-file fallback | OPEN | P2, bug — found via #2059 gate |

---

## 3. #941 DataFusion Design-A epic + children #1905–1914

| # | Title | State | Milestone | Priority |
|---|---|---|---|---|
| 941 | [EPIC] DataFusion table provider — Design A: co-located Flight-backed provider (Trino stays MPP owner) | OPEN | none | P3, epic |
| 1905 | A1: SnapshotScanManifest — versioned/signed schema | OPEN | none | P3 |
| 1906 | A2: Snapshot lease lifecycle; Trino stops production live-dir reads | OPEN | none | P3 |
| 1907 | A3: Streaming Flight producer — bounded RecordBatchStream, real cancellation, byte-cap budget | OPEN | none | P3 — "the hard blocker" |
| 1908 | A4: `cqlite-datafusion` crate (provider + exec) | OPEN | none | P3 |
| 1909 | A5: split planning + ring-coverage correctness | OPEN | none | P3 |
| 1910 | A6: Trino split model / affinity / dynamic filtering | OPEN | none | P3 |
| 1911 | A7: consistency-contract invariants (docs + fail-closed enforcement) | OPEN | none | P3 |
| 1912 | A8: E2E validation — DataFusion vs Trino over same epoch (capstone) | OPEN | none | P3 |
| 1913 | spike: Sidecar HTTP-Range go/no-go (gates Design B) | OPEN | none | P3 |
| 1914 | Design C future-epic placeholder (materialized epoch/Iceberg) | OPEN | none | P3, epic-candidate |

Claim: none of 1905–1914 have shipped anything — all still Backlog-shaped (P3, no milestone), dependency-ordered under 1905 first. #2605 (below) is a lighter-weight PoC spike layered on top of this same idea, scoped to 0.16.

---

## 4. #2037 ArrowMemtable epic + WS1–WS9

| # | Title | State | Milestone |
|---|---|---|---|
| 2037 | Exploration epic: ArrowMemtable — coordinator-native OLAP path | OPEN | none |
| 2043 | WS7 bench spike: measure #2037 flip-risk constants (nb decode, k-way merge ns/row, cache-format candidates) | OPEN | none |

**WS1–WS6, WS8, WS9 are NOT filed as separate issues.** The epic body explicitly marks them
"DO NOT promote without owner" — they exist only as workstream descriptions inside #2037's body
(WS1 precedent survey, WS2 tail live-stream protocol, WS3 generation pinning, WS4 process-boundary
decision, WS5 coordinator query surface, WS6 per-generation Arrow cache design, WS8 OLTP-safety
re-bench, WS9 CEP pitch draft). Only WS7 was groomed into a real issue (#2043). A 0.17 filer touching
this territory should re-groom from the epic body, not assume WS2–WS9 issue numbers exist.

Non-goals stated in the epic (binding): no CDC, no second on-disk storage format, no `nb` replacement,
no raw JVM-external trie memory reads. Exit criterion = owner-approved decision packet, not an
implementation epic yet.

---

## 5. Named individual issues

| # | Title | State | Milestone | Priority | Claims / measured numbers |
|---|---|---|---|---|---|
| 2605 | spike(0.16): DataFusion TableProvider PoC over the flight scan path | OPEN | 0.16 | P2 | Bench vs row engine on R12 corpus, feature-gated, zero production wiring. Lightweight sibling to #941's Design-A track. |
| 2765 | flight/merge: process-global adaptive egress budget | OPEN | none | enhancement | Follow-up to #2600. `cap_per_merge = clamp(EGRESS_ROW_BUDGET/active_merges, MIN, 256)`. Baseline table in body: 80 threads → peak egress depth 8080, qps 190, p50 417ms (local Apple Silicon repro, trend not absolute). |
| 2600 | flight: characterize + relieve merge-egress channel backpressure | **CLOSED (shipped)** | 0.16 | P1 | R12's dominant saturation signal (3,505 queued @ 80-thread in field). Shipped: attributed backpressure to consumer-side drain latency; lever = adaptive egress budget, CAP=32 cut depth 5-8x at <10% qps cost (per MEMORY.md). PR #2766. Follow-ups #2765 (impl), #2771 (nits), telemetry #2772. |
| 2679 | trino-connector: plan-time split pruning for fully-bound partition keys | **CLOSED (shipped)** | 0.16 | P1 | Point reads previously fanned out ~48 DoGets across all pods; fix PR #2810 per MEMORY.md #2806 field round. |
| 2680 | trino-connector: weight-balanced split→pod assignment | OPEN | 0.16 | P1 | floorMod rotation was count-balanced only → 2–4× pod CPU skew, B2 goal blocker. **CAUSED a live P0 regression (#2782, LIMIT hang) via its `sub-splits-per-range=4` default** — see below. Treat as NOT safely closed until #2782 resolved. |
| 2681 | flight: attribute do_get server-side 'other' errors (0.89% in soak) | **CLOSED (shipped)** | 0.16 | P2 | Fine-grained abort categories + abort-path trace. Follow-up nits #2787 (2 low-severity, open). |
| 2349 | Wire UDT registry into BOTH flight read paths | **CLOSED (shipped)** | 0.16 | P2, bug | UDT-in-collection decode previously went through non-registry branch. |
| 2371 | Flight: shadowed data never crosses real do_get transport; no TTL/range-tombstone seam | OPEN | none | P2 | Part of the "2371–2377 cluster" — E2E coverage-gap findings. |
| 2372 | BTI (`da`) has zero E2E coverage through Flight do_get and Trino testbed | **CLOSED** | 0.16 | P2 | Nit follow-up #2767 (open). |
| 2373 | Flight: compressed (chunk-stitching) BIG-`nb` never reaches do_get | **CLOSED** | 0.16 | P2 | Nit follow-up #2795 (open). |
| 2374 | Route the query-semantics oracle through Flight do_get as a parity lane | **CLOSED** | 0.16 | P2 | |
| 2375 | Flight: `DoGetInput::Aggregate` branch never exercised through real do_get | OPEN | none | P2 | |
| 2376 | Flight transport E2E for #2352 clearSnapshot regression + generation turnover mid-scan | OPEN | none | P2 | |
| 2377 | Testbed/loadtest E2E gaps: no shadowed data, no point-lookup/sparse-predicate shapes | OPEN | none | P2 | |
| 1536 | CLI `cqlite compact` without `--now-sec` silently skips TTL expiry | OPEN | none | P2, enhancement | Not really scan-path throughput — correctness/CLI contract issue, tangential to this program. |
| 2059 | Global bounded key→partition-offset cache | **CLOSED (shipped)** | — | — | Cassandra key-cache model; bounds aggregate across all open readers. Nits #2565 open. |
| 1644 | K5 — Zero-copy value extraction (Bytes-backed Value) | **CLOSED** | 0.15 | P1 | Joint Value-v2 train with E1/E3. Residual: #2597 (Tier-2 large-payload retention imperfect, open bug). |
| 1818 | B-series follow-up: BIG point-read cache dead on public query path | **CLOSED** | 0.15 | P3 | |
| 2210 | F6.1: MADV_RANDOM dedicated point-read mmap + cold-cache A/B harness | **CLOSED** | 0.15 | P3 | |
| 2319 | Direct I/O windowed scan allocates bounce buffer per chunk | **CLOSED** | 0.15 | P3, performance | Regressed reused-window from #1940 substrate rework. |
| 2096 | read-path: partition-seeking merge run reader for multi-candidate point reads | **CLOSED** | 0.15 | P2, performance | D3 follow-up. |
| 2230 | KWayMerger::step materializes entire partition | **CLOSED** | 0.15 | P2, bug/performance | LIMIT/batch_size didn't bound intra-partition memory; cancellation partition-granular. |
| 2423 | point-read (WHERE pk=?) and cache-warm merges still materialize whole partition | **CLOSED** | 0.15 | P1, bug/performance | Same defect class as #2230, more common wide-partition shape. |
| 2412 | Lazy Summary-guided partition index for BIG open | **CLOSED (shipped)** | 0.15 | P1 | O(summary) open, O(log n + interval) lookup, ~0 resident index — "the #2385 real fix." Headline 0.15 ship. |
| 2413 | flight scans: push split's token range into per-SSTable partition walk | **CLOSED (shipped)** | 0.15 | P1, performance | Every scan previously decoded ALL partition bodies from ring start (#2398 fix). |
| 2420 | WS4 (epic #2313): admission control / backpressure — bounded concurrent do_get | **CLOSED (shipped)** | 0.15 | P2, performance | tonic concurrency_limit + Semaphore, `--max-concurrent-scans 64`. Nits #2432 open. |
| 2782 | **P0: LIMIT queries hang (180s timeout) through cqlite_flight after #2680** | **OPEN** | none | P0 | Live regression on main (f5dd215a7). `DEFAULT_SUB_SPLITS_PER_RANGE=4` (#2680's default) + LIMIT + Trino early-termination = hang; evidence points at early-close path not draining a sub-split's DoGet stream. 3 remediation options on the table (revert / K=1 default / fix-forward); process-gap note: Flight↔Trino E2E is not a `required` check, so it didn't block auto-merge. Directly referenced by the program as a release-readiness hazard. Follow-up #2792 (make the E2E lane required, P1, open). |

---

## 6. Sweep — additional open issues matching perf|throughput|scan|flight|arrow|latency|egress|cache|io|memory|split|merge|scheduling|connector

Full keyword sweep of all open issues found **111 matches**; most are nit-batches/follow-ups on
already-closed parents (already folded into the sections above). Straggler items not yet covered
above, grouped by theme:

**Flight/connector coverage gaps & hardening (open, unmilestoned — same cluster as 2371–2377):**
- #2792 P1 — make Flight↔Trino E2E a required check (direct #2782 escape-hatch fix)
- #2378 P3 — no test of page-source close/cancel tearing down the Flight stream
- #2333 P3 — flight point-read follow-ups (#2207 nits): OR-of-IN flattening, tombstone-through-transport
- #2339 P2 — decode composite (frozen tuple/UDT) collection keys in merged-read assembly
- #2325 P2, bug — empty-string partition key writes OK, reads back corrupted via both scan paths
- #2322 P2, bug — sequential_scan byte-pattern header-resync silently loses rows on corrupt partition body
- #2368 P3, bug — SSTableWriter doesn't enforce canonical (token,key) order for equal-token collisions

**Cache/allocation nits (parents closed, hygiene open):**
- #2565, #2561 (key-cache, #2059 family — see §2)
- #2311 P3 — arrow_convert #1495 follow-up: transpose peak-memory + reorder-test nits
- #2213 — Murmur3 recomputed per-comparison on same-partition no-op path (#1917 follow-up)
- #2617 P3 — R3 follow-up: allocation-free complex-cell path

**Write/merge/compaction (adjacent, not scan-path but same "throughput" umbrella):**
- Epic Q #1610, Epic R #1611, Epic S #1612, Epic T #1613 (all OPEN, P1/P2 epics — write-path allocation
  discipline, streaming metadata writers, campsite split; NOT yet started, unmilestoned)
- #1956 P3 — mutation size estimated twice on accepted write path
- #1989 P3 — offline compactor hardening: throughput throttle

**Bindings/runtime perf (Node/Python, tangential to scan-path but same throughput theme):**
- #1901 P1 — move Node streaming off libuv threadpool onto tokio runtime
- #1883 P2, 0.17 (already milestoned) — Rust-side per-row alloc budget
- #1845 P1 — dev/test debuginfo diet (build perf, not runtime)
- #1851 P1 — compress Python/Node parity suites (build/test perf)

**Spark connector track (separate connector, same "connector throughput" family):**
- #1947–#1950 (S1–S4, all P3, unmilestoned) — connector-commons extraction, DataSourceV2 read connector,
  pushdown phase 1, E2E rig. None started.

**Observability (adjacent — "why is this slow", not itself a throughput fix):**
- Epic AI #1686 + children #1701/#1705/#1707 (dead read metrics, catalog drift, why-slow phase timings) — OPEN, unmilestoned/0.17

**CI/gate infra affecting this program's velocity (not product perf, flag for awareness only):**
- #2662 P1 — gate.yml nightly deep-check dead all July (75m timeout kills every run)
- #1894 P2 — define explicit CI merge lane-set + rate-limit-aware polling

**Research/exploration epics (large, unmilestoned, owner-gated — do not re-file):**
- #1934 P2, epic — CQLite two-project split exploration
- #1951 P3 — cross-replica quorum-merge for Flight connectors (CL.ONE → blockFor-replica) decision packet

---

## 7. 0.16 milestone perf-relevant open items (already scheduled — NOT 0.17 candidates)

These are open, in the 0.16 milestone, and directly perf/throughput-relevant. A 0.17 filing must
NOT duplicate these; they are already scheduled work for the current milestone.

| # | Title | Priority |
|---|---|---|
| 2680 | trino-connector: weight-balanced split→pod assignment (2–4× pod CPU skew, B2 blocker) — **regression-source of live P0 #2782** | P1 |
| 2605 | spike(0.16): DataFusion TableProvider PoC over the flight scan path | P2 |
| 1478 | AB3 — CLI one-shot `-e SELECT` streaming | P1 |
| 1479 | AB4 — read-sstable/export-sstable/read slurp | P2 |
| 1480 | AB5 — import streaming | P2 |
| 1481 | AB6 — `--out table` bound | P3 |
| 1482 | AB7 — batch ParquetWriter row-group cap | P2 |
| 1484 | AB9 — delta-scan windowed driver | P2 |
| 1497 | AE3 — delta emit hoist sets + index cell dispatch | P2 |
| 1498 | AE4 — delta emit intern column names | P3 |
| 1500 | AE6 — Node exportParquet spawn_blocking | P2 |
| 1472 | AA2 — Flight ticket size cap | P3 |
| 1490–1494 (open remainder) | AD parity/determinism (most closed; check live state before filing) | P1–P2 |
| 1502–1505 | AF1–AF4 dead-code/feature-hygiene/campsite (perf-adjacent, not perf-primary) | P2–P3 |

**#2782 (P0, LIMIT hang via #2680) has no milestone set** despite being a live regression referenced
by this program as a release-readiness hazard — worth flagging to the owner/lead; it should probably
be 0.16 given it's blocking on a 0.16 change already on main.

---

## Collision watchlist — where a new 0.17 filing is most likely to duplicate existing work

| Topic | Existing coverage a 0.17 filer MUST check first | Risk |
|---|---|---|
| **Caching** (block/chunk/key/result caches) | Epic B #1514 (B1–B5, all closed) — shared bytes-bounded chunk cache, key→offset cache (#2059, closed), decompressor cache. Nits open: #2565, #2561 (BTI chunk-straddling gap found via #2059's own gate). | HIGH — any "add a cache" filing likely re-treads B1–B5; check #2561 first, it's a real live gap in the shipped cache. |
| **Arrow encode / conversion cost** | Epic AE #1470 (AE1–AE6, 3 open: #1497/#1498/#1500), Epic K #1604 (closed), arrow_convert nits #2311. Epic AK/AJ campsite tracking (#1655, #1711). | MEDIUM — AE3/AE4/AE6 are the actual open remainder; a new filing should extend these, not restart. |
| **I/O (mmap, direct I/O, syscalls)** | Epic F #1518 (F1–F6, closed) incl. F6.1 MADV_RANDOM (#2210, closed), direct I/O bounce-buffer regression #2319 (closed but flags this is fragile — regressed once already from #1940). | MEDIUM — regression-prone area; check #2319's fix is still intact before assuming it's solved. |
| **Merge (k-way merge, partition materialization)** | #2230 + #2423 (closed, same defect class — partition-granular materialization/cancellation), #2096 partition-seeking reader (closed), Epic Q #1610 (compaction allocation discipline, open/unstarted). | HIGH — #2765 (adaptive egress budget) is the CURRENT open front here; a 0.17 filer on "merge memory bound" should extend #2765, not refile. |
| **Connector (Trino split planning/scheduling)** | #2679 (closed — split pruning), #2680 (open — weight-balanced splits, **currently regressing prod via #2782**), df-provider #1905–1914 (all open, unstarted, P3), #2605 (0.16 DataFusion PoC spike). | HIGH — #2680/#2782 is an active fire; do not file a new "connector scheduling" issue without reading #2782 first. Spark connector #1947–1950 is a parallel, separate connector — don't conflate. |
| **Scheduling / admission / backpressure** | Epic F4 admission control (closed), #2420 flight admission control (closed, nits #2432 open), #2600/#2765 egress backpressure (shipped char. + open impl follow-up). | HIGH — #2765 is the single open front; extend it. |
| **Flight transport E2E coverage** | The #2371–2377 cluster (3 closed, 4 open: 2371/2375/2376/2377) + #2792 (make E2E required, direct #2782 fallout). | MEDIUM — a "let's test Flight E2E better" filing should map onto whichever of 2371/2375/2376/2377 it actually is, not restart the cluster. |
| **DataFusion / TableProvider** | #941 + #1905–1914 (full design epic, unstarted) AND #2605 (lighter 0.16 spike PoC) are TWO overlapping tracks already. | HIGH — a 0.17 filing here needs to explicitly reconcile against BOTH #941's Design-A dependency chain and #2605's spike results, or it triples the surface. |
| **ArrowMemtable / coordinator-native OLAP** | #2037 (exploration epic, WS1–9 mostly unfiled) + #2043 (WS7 spike, open). | LOW for 0.17 (owner-gated exploration, "do not promote without owner") but HIGH for accidental scope creep if a 0.17 perf issue reads like it's proposing WS2–WS6. |

**File path:** `/Users/patrickmcfadin/local_projects/cqlite/docs/research/throughput-backlog-inventory-2026-07.md` (left uncommitted per instructions).
