# Phase 1 — Caching levers for the CQLite throughput program

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Agent:** Phase-1 4/8 (CACHING) ·
**Method:** READ-ONLY code + issue survey. No builds, no commits, no GitHub writes.

Anchored to `docs/research/phase0-scan-cost-breakdown-2026-07.md` (the CPU breakdown) and
`docs/research/throughput-backlog-inventory-2026-07.md` (the dedup map). Every multiplier here
traces back to Phase-0's stage shares; every collision traces to the inventory's cache watchlist.

---

## 0. TL;DR — the one thing to internalize

**Caching is a keyed-read / repeated-query lever, NOT a scan lever.** Phase-0 proved a single-stream
full scan spends **~82 % of CPU in merge plumbing** (49.9 % per-row channel park/wake + 32.5 % k-way
reconcile) that **no data cache can touch**, and only **~14 % in the stages a cache buys back**
(9.7 % parse + 4.5 % materialize; IO+decompress ≈0 % locally because the rig is warm+uncompressed).
So:

- **For a true one-off full scan, every decoded/chunk/key/row cache has hit-rate ≈ 0 — useless.** No
  optimism theater: a scan reads each partition exactly once, evicts its own working set, and the
  merge cost it is bottlenecked on is uncacheable. The only scan-relevant cache is the **warm-reader
  cache (already shipped, #2310)** which amortizes *open* cost, and — for the field's LZ4+cold reality
  Phase-0 is blind to — a chunk cache buys back decompress+IO **only on repeated scans of the same
  token range within a freshness window**.
- **For keyed reads (A2 ≥1,000 qps/pod) and repeated concurrent queries (B2), caches are
  transformative** — high temporal locality, the per-read cost is index+IO+decompress+parse+materialize
  with little/no merge, and a hot-hit removes nearly all of it. This is the workload to optimize.

**B4 is already in tension before we add anything.** The three shipped resident caches declare
**256 MiB (chunk) + 64 MiB (global key) + 64 MiB (warm readers) = 384 MiB** of budget against a
**512 Mi peak** ceiling — and **none of them drain at idle** (verified: no TTL/decay/idle-sweep on any
of the three; the warm registry's `last_access` drives *budget-LRU only*, not a time-based drain). A
pod that serves one scan and goes quiet holds its last cache contents indefinitely. **The idle-≤16 Mi
half of B4 is therefore not met by construction today**, and every new cache worsens it. Any lever
below that adds resident bytes MUST ship an idle-drain or it is a B4 regression — this is the gating
constraint, restated per lever.

---

## 1. Inventory — what is and isn't cached today

### 1a. Shipped caches (read the code)

| # | Cache | Issue | What it caches | Scope / budget | Wired sites | Idle-drain? |
|---|-------|-------|----------------|----------------|-------------|-------------|
| 1 | **`DecompressedChunkCache`** (B1) | #1567 / Epic B1 | **post-decompress** SSTable chunk bytes (`Bytes`, refcount-bump hit) keyed by `(sstable_id ^ site_salt, chunk_index, aux=size)` | **per-manager**, byte-bounded, sharded ×16 `Mutex<LruCache>`; default **256 MiB** (`block_cache.max_size = max_memory/4`), configurable; `disabled()` = genuine no-op | BTI target-chunk (`ChunkSource::chunk`), windowed scan (`decode_borrowed`), BIG point-read (`get_cached_data`, aux=size). **NOT** wired on `iterate_all_partitions`/`sequential_scan` legacy decode → follow-up **#2165** | **No** |
| 2 | **`GlobalKeyOffsetCache`** | #2059 / Epic B | partition **key → `PartitionLoc{offset,size}`** (skips the `Index.db` interval read on hit; BIG resolves offset+size, BTI offset-only) | **process-global singleton**, byte-bounded, sharded ×128; **FIXED 64 MiB** (ignores `max_size` by design §B); invalidate-by-generation-identity (inode-stable, #2345/#2383) | every `SSTableReader` open with block-cache enabled; consulted before index descent | **No** |
| 3 | **`ChunkDecompressor.chunk_cache`** (B3) | #1569 / Epic B3 | post-decompress chunks (`Arc<[u8]>`) keyed by chunk index | **per-reader**, single-threaded, **16 entries ≈ 256 KB** fixed `LruCache`; legacy `BulletproofReader` stack (being consolidated onto ChunkSource, G-series #1598/#2165) | legacy `read_data` decode path | n/a (tiny, per-reader; dies with reader) |
| 4 | **`WarmTableRegistry`** | #2310 | **`Arc<SSTableReader>`** (open FDs + parsed Summary/lazy-BIG-index #2412 + BTI trie) per `(keyspace,table)` generation set | **per Flight process**, byte-bounded LRU, **64 MiB**; generation-keyed; rebind-by-inode (#2383); refreshed by probe/manifest | Flight `do_get` warm path — a warm hit amortizes **O(summary) open (#2412) to ~0** | **No** (LRU under budget only) |
| 5 | **`SnapshotManager` reuse** (connector) | #2356/#2306 | the **snapshot directory identity** per `(keyspace,table)` within a freshness window (NOT data) | Trino JVM; **3 s window** (`DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS=3000`), retire-grace **10 min**, TTL **6 h** backstop | Trino split planning → N queries/window pay ONE create fan-out (one flush/host) | window-scoped |

### 1b. Supporting / non-data caches (for completeness — not throughput levers)

- **Query plan cache** (`engine.rs` `plan_cache: DashMap`, `prepared.rs` `plan_cache`) — caches
  *parsed+optimized query plans* by query text. Saves re-parse/re-optimize, not data. Irrelevant to
  scan/keyed data throughput.
- **`PartitionKeyCache`** (`row_build.rs`) — a **per-stream single-entry memo** of the last decoded
  partition key's columns, making a partition-grouped scan decode PK columns O(partitions) not
  O(rows). NOT a cross-query cache; a working-set optimization inside one scan. (It is also the
  SipHash-per-row site Phase-0 flagged at ~4.5 %.)
- **`type_cache`** (write path, parsed `CqlType`) — write-only, irrelevant to reads.
- **Connector `HostSnapshotApis`** — per-host HTTP `SnapshotApi` client reuse (connection pooling).
- **Connector `CqliteFlightMetadata` nonAggregatedStats** — memoized `TableStatistics` (incl. negative
  `empty()`) per `SchemaTableName`; plan-time, not data.

### 1c. What is NOT cached anywhere today (the real gaps)

- **No decoded-row / decoded-partition cache across queries.** The only decoded-form memo is the
  per-stream single-entry `PartitionKeyCache`. A repeated point read re-parses and re-materializes the
  partition body every time even when its chunk is resident in B1. **This is the biggest keyed-read
  gap.**
- **No Arrow `RecordBatch` / page cache** — server-side or connector-side. Every `do_get` rebuilds
  batches from rows.
- **No Trino-worker split/page result cache** — repeated identical splits within the 3 s snapshot
  window re-fetch and re-decode end to end.
- **No separate "uncompressed-chunk (post-LZ4, pre-decode)" tier** — and there shouldn't be: LZ4
  decompress *is* the decode, and **B1 already caches the decompressed (uncompressed) chunk buffer**,
  which is exactly Cassandra's own `ChunkCache` model (Cassandra caches decompressed chunk buffers,
  not compressed ones). The "pre-decode" framing has no distinct artifact to cache here.

### 1d. Tried-and-reverted / cautionary history (`git log --grep`, inventory watchlist)

- **Epic B (#1514) already shipped the whole read-cache family** (B1 chunk, B3 decompressor,
  B4 key→offset, B5 observability) — a new "add a cache" filing re-treads closed ground. Confirmed via
  `git log`: #2170 (one ChunkSource decode plane), #2513 (pin BIG point-read served from B1 cache),
  #2554 (#2059 global key cache), #2320/#1940 (window-as-`Bytes` zero-copy substrate).
- **No cache was tried-and-reverted**, but there are **live gaps in the shipped caches**:
  - **#2561** (P2, bug) — BTI point-read chunk-straddling decode trusts a closure-fired-as-complete
    signal → whole-file fallback; found *via #2059's own gate*. A correctness gap in the cached path.
  - **#2565** — #2059 doc/test-hygiene nits (open).
  - **#2165** — `iterate_all_partitions`/`sequential_scan` decode still bypasses B1 (the scan path
    most relevant to full scans is the one NOT wired to the chunk cache).
- **#302f1927 lesson:** #2059's global cache *broke a per-reader cold-start test assumption* (serial-order
  coin flip) — a process-global cache leaks state across "independent" reads/tests. Any new
  process-global decoded cache inherits this hazard: tests need explicit `invalidate_all()` cold-start.
- **#2316/#2321 lesson:** merge producer-thread cost is already the throughput limiter — caches that
  add per-read background threads/timers (an idle-drain sweeper) must stay off the hot path.

---

## 2. Workload split — why the two targets behave oppositely

| | **Scan (A4 / B3 full-ring, one-off)** | **Keyed read (A2 ≥1k qps/pod) + repeated concurrent (B2)** |
|---|---|---|
| Access pattern | each partition read **once**; working set = whole table (248 MB local, **GB-scale field**) | small hot set of partitions/dashboards read **repeatedly** within the snapshot window |
| Temporal locality | ~0 (except **Trino re-scanning the same token range** across splits / repeated dashboards) | **high** |
| Phase-0 cost profile | 82 % merge plumbing (uncacheable) + ~14 % parse/materialize + field-only decompress/IO | index lookup + IO + decompress + parse + materialize; **little/no merge** for point reads |
| Cache hit rate | **≈0 one-off**; only repeated-range scans within a window hit | **high** — this is where caches pay |
| Multiplier ceiling from caching | **~1.16× local** (1/(1−0.14)); **~1.3–1.5× field** if LZ4 decompress is 20–30 % of a cold scan — but only realized on *repeated* scans | **2–10×** on the hot set (a hot hit removes nearly the entire per-read cost) |
| Memory pressure vs B4 | catastrophic if you try to cache scan output (decoded rows are ~3.5× on-disk per Phase-0 wire ratio) → **thrash under 512 Mi** | modest — hot set is small; fits a bounded cache |

**Rule:** size and invalidate every cache for the **keyed/repeated** workload; treat scans as
**cache-transparent** (bypass or single-touch, never let a scan evict the hot keyed set — a
scan-resistant admission policy, see §4 Lever G).

---

## 3. Lever table — SCAN workload (A4 / B3)

Peak-memory math is **per Flight pod** against **B4: ≤512 Mi peak, ≤16 Mi idle**. "Multiplier" is the
*realized* scan speedup, not the theoretical ceiling.

| Lever | What it caches | Hit-rate model (field: 1.93 M part/node, GB-scale) | Peak-mem math vs 512 Mi | Idle-≤16 Mi story | Multiplier (scan) | Cost | Risk | Collisions |
|-------|----------------|-----------------------------------------------------|--------------------------|-------------------|-------------------|------|------|------------|
| **S-A. Wire B1 chunk cache into `sequential_scan`/`iterate_all_partitions`** (#2165) | post-decompress chunks on the full-scan decode path (today it bypasses B1) | **~0 for one-off** full ring; **useful only when Trino re-scans the same token range** within a window (repeated dashboards, multi-split overlap). Field LZ4 decompress is real (Phase-0 §5.1), so a repeated-range hit buys back real decompress+IO | reuses the existing 256 MiB B1 budget — **no new bytes** IF sized down (see S-D) | inherits B1 (no drain today — **must add S-D**) | **1.0× one-off; ~1.3–1.5× on repeated-range scans** (field, decompress-bound) | **M** | scan blows a bounded cache → churns the keyed hot set unless scan-resistant (Lever G). Correctness parity on the legacy path | **#2165** (exact issue). Extends Epic B, does not refile |
| **S-B. Longer snapshot-reuse window** (connector tuning) | raises reuse of the *whole downstream stack* (warm readers + B1 + key cache stay hot for the same generation longer) | monotonic in window length: a 3 s→30 s window turns N-queries-per-3 s into N-per-30 s → fewer create fan-outs (fewer flushes) AND higher warm/B1/key hit rates | **0 new bytes in the Flight pod**; more *retained superseded snapshot dirs* on disk (≈ grace/window, disk not RAM) | unaffected (disk, not the 16 Mi RAM ceiling) | indirect — lifts keyed multipliers more than scan; modest scan lift on repeated ranges | **S** (config knob `cqlite.snapshot-reuse-window-ms` already exists) | **freshness contract** — data up to window-old; owner call (mirrors #2305 flush-on-snapshot decision) | Tuning, not code. No issue collision; owner-gated freshness decision |
| **S-C. Expanded warm-reader/summary caching** | more generations' `Arc<SSTableReader>` (open FDs + lazy summary #2412) resident | warm-open already ≈0 cost on hit (#2412 made open O(summary)); expansion only helps when the working set of *generations* exceeds the 64 MiB reader budget | raising the 64 MiB warm budget adds directly to peak; readers hold FDs + parsed summary (tens of KB–MB each) | **No drain today** — readers persist idle (FD leak risk, see #2013 soak) | **~1.0–1.05×** (open is already amortized) — low value | **S–M** | FD exhaustion under many generations; #2013 resource-leak class | Extends #2310; low priority |
| **S-D. Idle-drain / TTL decay on all resident caches** (B4 enabler) | nothing new — *drains* B1 + key cache + warm readers after an idle interval | n/a (memory lever, not a hit-rate lever) | **the lever that MAKES the 384 MiB of existing budget B4-legal** — drains toward ≤16 Mi when a pod goes quiet | **this IS the idle story** for every other lever | 0× throughput; **unblocks** every resident-cache lever under B4 | **M** | drain sweeper must stay off the hot path (#2316 lesson); a too-eager drain re-cold-starts a briefly-idle pod (thrash under bursty B2) | **New** — no existing issue owns "cache idle-drain"; prerequisite for any resident-cache growth. Pairs with #2013 |

**Scan verdict:** the only genuinely useful *new* scan work is **S-A (#2165)** — and it pays only in
the field (LZ4) on repeated ranges, capped at ~1.3–1.5×. **S-B (window tuning)** is the cheapest lever
in the whole program and lifts keyed reads more than scans. **S-D is not optional** — it is the B4
gate. Do not build a scan-output/row cache: it cannot beat the 82 % merge wall and it detonates B4.

---

## 4. Lever table — KEYED / CONCURRENT workload (A2 ≥1k qps/pod, B2)

This is where caching earns its multiplier. Hit rate assumes a **hot set that fits the budget** (the
field's dashboards hit a small partition subset repeatedly — the classic keyed workload).

| Lever | What it caches | Hit-rate model | Peak-mem math vs 512 Mi | Idle-≤16 Mi story | Multiplier (keyed) | Cost | Risk | Collisions |
|-------|----------------|----------------|--------------------------|-------------------|--------------------|------|------|------------|
| **K-A. Decoded-partition cache** (NEW, the biggest gap) | post-parse, post-materialize partition columns/rows keyed by `(GenerationIdentity, partition key)` — reuses the #2059 inode-stable identity for **safe invalidation** | **high on the hot set** — a repeated point read that today re-parses+re-materializes (Phase-0 stages 2+3 = ~14 % CPU, plus the alloc + SipHash it drives) becomes a clone | **decoded rows are ~3.5× on-disk size** (Phase-0 wire ratio) → budget must be **small (e.g. 32–64 MiB) and hot-set-only**, NOT scan-populated (needs Lever G scan-resistance) | **No drain unless S-D** — decoded bytes are the most expensive to hold idle → S-D mandatory | **removes parse+materialize+alloc+SipHash on a hit** → on top of an already-warm chunk/key hit, a point read collapses to a cache clone. **~1.5–3× on the hot keyed set** (larger when parse/materialize dominate wide rows) | **L** | invalidation on generation turnover (identity handles it, fail-closed like #2059); **memory blowup** (3.5× factor); process-global cold-start test hazard (#302f1927 lesson) | **HIGH collision with #2037 ArrowMemtable** (coordinator-native decoded/Arrow cache — the *same idea*, owner-gated exploration). A 0.17 filing here MUST reconcile with #2037 WS6 (per-generation Arrow cache) or it triples surface. Also overlaps #2605 DataFusion PoC |
| **K-B. Global key cache — already shipped** (#2059) | key→offset; skips `Index.db` interval read on hit | **high** on repeated keyed reads; fixed 64 MiB holds >1 M hot locations | 64 MiB fixed (already counted) | **No drain** (needs S-D) | **already realized** — skips the index descent (a real per-point-read cost). Baseline, not a new lever | shipped | **#2561 (chunk-straddling whole-file fallback bug)** undermines the win on straddling partitions; **fix #2561 first** | Fix #2561/#2565; don't refile |
| **K-C. B1 chunk cache — already shipped** (#1567) | post-decompress chunk holding the partition body | **high** on repeated keyed reads (partition body chunk resident → refcount-bump, skips IO+decompress) | 256 MiB default — **oversized for a 512 Mi pod**; size to the hot set (S-D + a smaller `block_cache.max_size`) | **No drain** (needs S-D) | **already realized** for point reads; buys back the field's LZ4 decompress | shipped | 256 MiB default vs 512 Mi peak is the single biggest B4 hazard — **retune the default down for the Flight/Trino deployment** | Config retune, not refile; pairs with S-D |
| **K-D. Snapshot-scoped decode cache** (the safe-invalidation design for K-A) | K-A's decoded entries keyed/bucketed by **snapshot identity** so invalidation = "snapshot rolled → drop the bucket" (immutable snapshot ⇒ no per-entry invalidation) | same hit-rate model as K-A; **cleaner invalidation** — a snapshot is a Cassandra point-in-time hardlink set, immutable, so a bucket is valid until the generation set changes | same as K-A | drop-on-roll gives a **natural idle-drain** if paired with the 3 s window / warm-registry generation identity | same as K-A, with **lower invalidation risk** | **L** (do K-A this way if at all) | ties cache lifetime to snapshot lifecycle — correct-by-construction but couples core to the connector's snapshot notion | Same #2037 collision as K-A; aligns with #2356 snapshot model |
| **K-E. Trino-connector-side page/split cache** | Arrow pages / split results on the **Trino worker** (JVM), keyed by `(split, snapshot epoch)` | **high** on repeated identical splits within the 3 s window (dashboard refresh) | **lives in the Trino JVM, OUTSIDE the 512 Mi Flight-pod B4 ceiling** — Trino's own memory management, not the pod idle-16 Mi budget | n/a to the Flight pod; Trino manages its own eviction | avoids the whole Flight round-trip (network + decode + merge) on a repeat split → **large for repeated dashboards**, but Trino may already cache upstream | **M** (Java connector work) | correctness on snapshot roll (epoch key handles it); may duplicate Trino's built-in caching layers | No cqlite issue; separate connector track. Don't conflate with Spark #1947–1950 |
| **K-F. Longer snapshot-reuse window** (= S-B, keyed view) | keeps the whole warm+B1+key stack hot for the same generation longer | **directly multiplies keyed hit rates** — the hot set stays resident across more queries | 0 new pod bytes | window-scoped | biggest cheap lift for A2/B2 | **S** | freshness contract (owner call) | Existing knob; owner-gated |
| **K-G. Scan-resistant admission (SLRU / scan-bypass)** | not a new cache — an **admission policy** so a full scan does not evict the keyed hot set from B1/K-A | n/a — protects the *other* caches' hit rates under mixed scan+keyed load (A5: eviction under 80-thread overload must not thrash) | 0 new bytes | improves idle behavior (scan bytes never pinned) | protects the 2–10× keyed multiplier from collapsing to ~1× when a scan runs concurrently | **M** | mis-tuned admission starves legitimately-hot scanned ranges | Cassandra prior art (ChunkCache is scan-aware); no cqlite issue. Pairs with #2600/#2765 egress work |

**Keyed verdict:** the shipped **K-B + K-C** already deliver the index+IO+decompress skip — **first fix
#2561 (they're partially broken on straddling partitions) and retune the 256 MiB chunk default down**
before anything new. The one high-value *new* lever is **K-A/K-D (decoded-partition cache, snapshot-scoped)**,
but it is **L-cost, B4-dangerous (3.5× decoded size), and collides hard with the #2037 ArrowMemtable
exploration** — it should be reconciled with #2037/#2605, not filed independently. **K-F/S-B (window
tuning) is the cheapest keyed win in the program.** **K-G (scan-resistance) is the A5 safety belt** that
keeps mixed workloads from thrashing.

---

## 5. Recommended sequencing (cheapest, highest-leverage first)

1. **Fix the shipped caches before adding any** — **#2561** (BTI chunk-straddling whole-file fallback,
   a real correctness+perf hole in K-C found via #2059's gate) and retune **`block_cache.max_size`**
   down from 256 MiB for the 512 Mi Flight/Trino deployment. **No new code, closes a B4 hazard.**
2. **S-B / K-F: tune the snapshot-reuse window** (owner freshness call). Config-only; lifts keyed hit
   rates and cuts create fan-outs. Highest ROI in the program.
3. **S-D: cache idle-drain / TTL decay.** The B4 gate — without it the existing 384 MiB of budget
   already misses idle-≤16 Mi and no resident-cache lever is legal. Off the hot path (#2316).
4. **S-A / #2165: wire B1 into the sequential-scan path.** Field-only (LZ4) repeated-range payoff;
   pair with **K-G scan-resistance** so it doesn't evict the keyed hot set.
5. **K-A / K-D: decoded-partition cache (snapshot-scoped)** — only after reconciling with **#2037
   ArrowMemtable** and **#2605 DataFusion PoC**. Highest keyed multiplier, highest cost/risk. Do not
   file standalone.
6. **K-E: Trino-side page/split cache** — separate connector track; evaluate against Trino's own
   caching first.

## 6. Hard constraints restated (do not violate)

- **B4 (512 Mi peak / 16 Mi idle) is a ceiling, not a trade chip.** Current declared budget 384 MiB with
  no idle-drain ⇒ idle-16 Mi is unmet today. Every resident-cache lever must state peak-mem math and
  ship an idle-drain (S-D) or it is a regression.
- **A5 (no thrash under 80-thread overload):** eviction must be scan-resistant (K-G) and lock-sharded
  (both shipped caches already shard ×16/×128) so the hit path never serializes.
- **No-heuristics (#28):** every proposed key (`GenerationIdentity`, chunk index, snapshot epoch) is
  authoritative — never inferred from decoded byte content. K-A/K-D reuse #2059's inode-stable identity.
- **Scans cannot beat the 82 % merge wall** (Phase-0). Caching's scan ceiling is ~1.16× local /
  ~1.3–1.5× field, realized only on repeated ranges. Do not sell caching as a scan fix.

**File path:** `/Users/patrickmcfadin/local_projects/cqlite/docs/research/phase1-4-caching.md`
(uncommitted per instructions).
