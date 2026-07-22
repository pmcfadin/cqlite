# Phase 1.5 — Transport & worker ingest: decomposing the through-Trino throughput gap

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Agent:** Phase-1 agent 5/8 (transport & worker ingest)
**Anchors:** `docs/research/phase0-scan-cost-breakdown-2026-07.md` (server-side CPU),
`docs/research/throughput-backlog-inventory-2026-07.md` (dedup), R12 field round (#2367).

READ-ONLY survey. Every claim below is `file:line`, a field measurement, or cited prior art, and is
marked **[CODE]**, **[FIELD]**, **[DERIVED]** (arithmetic from the other two), or **[PRIOR-ART]**.
No workspace build was run (the one-microbench allowance needs a full cqlite-flight/Java build —
declined per the constraint); the connector costs are read off the source and the Arrow-Java API
contract, not measured, and are flagged **[DERIVED]** wherever a multiplier is quoted.

---

## 0. The two numbers, reconciled

| | rows/s | MB/s | source |
|---|---:|---:|---|
| **Local server-direct, single stream** | ~500 k / stream | ~150 (Arrow wire) | Phase 0 §2 **[FIELD]** — warm, uncompressed, RF=1, loopback, narrow `keyvalue` |
| **Field, through Trino (B3 / R12)** | **~10.6 k / pod** | **~1.9 / worker** | 941 brief: 1.94 M rows, 61.1 s, 3 pods **[FIELD]**; worker MB/s = rows/s × row-width **[DERIVED]** |

**The "1–2 MB/s/worker" is not an independent floor — it is the arithmetic image of 10.6 k
rows/s/pod.** 10.6 k rows/s × ~180 B/row Arrow-wire ≈ **1.9 MB/s** **[DERIVED]** (row width assumed
from the `cassandra_easy_stress.keyvalue` shape Phase 0 used: `key text` + `value text`, ~100–200 B
on the wire; if the R12 corpus is wider, MB/s rises and rows/s falls proportionally). So there is
**one gap to explain, expressed two ways**, and both the rows/s and the MB/s levers below move the
same underlying quantity. Do not treat page-building, the network, and rows/s as three separate
throughput problems — they are one drain chain.

**The drain chain (the unit of analysis):**

```
SSTable → decode → k-way merge → egress channel(#2600) → Arrow encode → gRPC/HTTP2 write
   → [network + HTTP/2 flow control] → Java FlightStream.next() → ArrowToTrino row-by-row page build → Trino Page
```

The slowest link in this chain, times the number of concurrent chains (streams) per pod, sets
10.6 k rows/s/pod. Phase 0 profiled the **first four links** (server CPU); this report owns the
**last five** (encode → wire → Java ingest) plus **how many chains run at once**.

---

## 1. The gap decomposition — where the ~47× goes

500 k rows/s (single local stream) ÷ 10.6 k rows/s/pod = **~47×**. It splits cleanly into a
**distribution** deficit (the pod runs too few effective streams) and a **per-stream ceiling**
deficit (each field stream is far slower than the local one). The R12 saturation telemetry is the
key that separates them.

> **R12 saturation snapshot [FIELD]** (941 brief, quoting #2367 / #2600): **admission 12/64**,
> **blocking pool 8/~512**, **egress queued 3,505 rows**. Read this precisely: the server was
> **≈5× under its admission ceiling and ≈64× under its blocking-pool ceiling** — it was **not**
> parallelism-bound — yet **rows were piling up at the egress channel**. The pipeline was
> **drain-limited, not produce-limited.**

### 1a. Distribution deficit — the pod runs too few, and stalling, streams (≈4–16×)

- **Fan-out model [CODE]:** the full-scan path emits **one split per token range**, each pinned to
  one replica (`CqliteFlightSplitManager.buildSplits`, `CqliteFlightSplitManager.java:288–337`).
  Trino's driver scheduler runs those splits concurrently up to `task.concurrency` per worker. Each
  split is **one** `CqliteFlightPageSource` → **one** `ReplicaFailoverStream` → **one** `do_get`.
- **admission 12/64 across 3 pods ⇒ ~4 concurrent `do_get` per pod [DERIVED].** Against a 64-permit
  ceiling and a ~512 blocking pool, the server has **5–16× idle parallelism headroom**. The pod is
  aggregating ~4 streams × ~2.6 k rows/s/stream ([DERIVED] 10.6 k ÷ 4), not 64 × anything.
- **No Java-side prefetch — each stream is internally serial [CODE].**
  `CqliteFlightPageSource.getNextSourcePage` (`CqliteFlightPageSource.java:45–64`) is a **synchronous
  pull**: it calls `stream.next()` (blocks for the next Arrow batch), then runs `ArrowToTrino.toPage`,
  then returns. While the page is being built the FlightStream is **not being pulled**, so the
  server's next batch has nowhere to go → the per-stream pipeline is `[wait batch] → [build page] →
  [Trino consumes] → [wait batch]` with **no double-buffering**. Page-build latency is injected
  directly into stream idle time. `ReplicaFailoverStream` (`ReplicaFailoverStream.java:67–121`) is
  the same synchronous `next()/getRoot()` with no async/prefetch/executor.
- **The aggregate path is worse but off the B3 critical path [CODE]:**
  `CqliteFlightAggregatePageSource` (`:83–92`) fans out **serially** — a `for` loop over ranges,
  one `do_get` at a time inside a single page source. B3 is a non-aggregate full scan, so the
  scalar path above governs; but any GROUP-BY/agg query is single-stream-serial today.

**Distribution verdict:** the pod is leaving **~4–16×** on the table purely to under-fan-out and
per-stream serialization, and #2600's egress queue (3,505) proves the few streams that *do* run
can't drain — so simply raising `task.concurrency`/split count without fixing the ceiling just moves
the queue.

### 1b. Per-stream ceiling deficit — each field stream is ~189× the local one (≈3–12× beyond what RF/network trivially explain)

500 k ÷ 2.6 k ≈ **189× per stream [DERIVED]**. Local is warm/uncompressed/RF1/loopback; field is
none of those. Multiplicative factors, **wide bands, honest confidence**:

| Factor | Est. multiplier | Confidence | Basis |
|---|---:|---|---|
| **Cold / partly-cold IO** (1.93 M partitions/node not resident; Phase 0 was page-cache-warm) | **2–8×** | **LOW** | Phase 0 §5.2 flags this as invisible on the warm rig; pure unknown, and likely the single biggest term |
| **LZ4 decompression** (field SSTables compressed; local was uncompressed → Phase 0 Stage-1 ≈0%) | **1.3–2×** | MED | Phase 0 §5.1 — "a whole decompression-CPU stage this profile is structurally blind to" |
| **Network transport + HTTP/2 flow control** (real send/recv + window stalls vs loopback ≈0%) | **1.5–3×** | MED | Phase 0 §5.3; §5 below (gRPC window is unconfigured) |
| **Connector page building + no Java prefetch** (row-by-row `toPage`, serial pull) | **1.5–5×** | MED | §2 below; `ArrowToTrino.java:107–141`, `CqliteFlightPageSource.java:45–64` |
| **Trino overhead** (split planning, exchange, driver sched, page processing) | **1.2–2×** | LOW | not in CQLite's code; generic MPP overhead |
| **RF=3** | **~1.0–1.2×** | MED | connector reads each range from **one** pinned replica (`buildSplits`), so RF3 does **not** 3× the row work; cost is fan-out/coordination only |

Product of the bands: ~9× (low) to ~360× (high), straddling the observed 189× — i.e. the factors are
individually plausible but **cannot be pinned without field profiling of the through-Trino path**
(no such profile exists; Phase 0 is server-direct only). The **narrow-row per-row coordination
ceiling** Phase 0 indicts (the cap-256 `sync_channel` handoff, 47–50% of server CPU) is *already
inside* the 500 k local number — it is the same on both sides and does not itself widen the gap; it
just means the per-stream ceiling starts low.

**Confidence-weighted attribution of the ~47× pod gap:**

| Bucket | Share of the 47× | Confidence | Actionable via |
|---|---|---|---|
| Distribution (under-fan-out + per-stream serialization + undrained egress) | **~30–50%** | MED | more streams (§2b), async prefetch (§2b), #2600/#2765 egress budget (§6) |
| Cold-IO + decompression (field-only, data-plane) | **~30–45%** | LOW | out of transport scope — read-path/caching epics (B/F, closed) + warm-reader residency |
| Transport + connector ingest (encode/wire/page-build) | **~15–30%** | MED | §2, §3, §4, §5 below — **this report's owned levers** |

---

## 2. Lever #1 — Connector page building (`ArrowToTrino`)

### 2a. What it does today, per cell [CODE]

`ArrowToTrino.toPage` (`ArrowToTrino.java:55–65`) → `toBlock` (`:107–120`) → `writeValue` (`:122–141`)
is a **row-at-a-time, per-cell dispatch** loop:

```java
for (int i = 0; i < rowCount; i++) {            // per row
    if (vector.isNull(i)) { builder.appendNull(); continue; }   // validity check
    writeValue(type, vector, i, builder);        // switch(type) → checkcast → get(i) → builder.write*
}
```

Per cell the JVM does, every row: (1) `vector.isNull(i)` bitmap probe; (2) a `switch (type)` Java-21
pattern match (`:123–140`) — a chain of type tests; (3) a `checkcast` to the concrete vector
(`(BigIntVector) vector`); (4) `vector.get(i)` — a bounds-checked buffer read; (5) an interface
dispatch `t.writeLong/writeSlice(builder, …)` into a `BlockBuilder` (capacity check + append). That
is **~5–10 ops + 1–2 virtual/interface dispatches per cell**, none of it hoisted out of the loop —
the type is re-decided for **every cell** even though it is constant for the whole column.

**The varchar/varbinary path additionally allocates and copies per cell [CODE]/[PRIOR-ART].**
`varcharSlice`/`binarySlice` (`:307–334`) call `VarCharVector.get(i)` / `VarBinaryVector.get(i)`,
and Arrow-Java's `BaseVariableWidthVector.get(int)` **allocates a fresh `byte[]` and copies the cell
out of the off-heap buffer** ([PRIOR-ART] — Arrow-Java API contract; the zero-copy accessor is
`get(NullableVarCharHolder)` / direct `ArrowBuf` slicing, not used here). For the 2-varchar
`keyvalue` row that is **2 heap `byte[]` allocations + 2 copies per row** → **~3.9 M short-lived
`byte[]`/scan** on the worker, straight into the young-gen allocator and the exact per-cell-alloc
anti-pattern Phase 0 flags on the *server* side.

The FixedSizeBinary→UUID path (`formatUuid`, `:337–344`, `varcharSlice` `:326`) is worse still —
`HexFormat` + 4 `String` concatenations per uuid cell — but `keyvalue` doesn't hit it.

### 2b. Near-zero-copy mapping to Trino blocks [PRIOR-ART]/[DERIVED]

Arrow's in-memory layout and Trino's `io.trino.spi.block` layout are close cousins; a **bulk,
per-column** conversion replaces the per-cell loop:

- **Fixed-width (int/bigint/double/real/date/timestamp):** Arrow's fixed-width vector data buffer is
  a contiguous little-endian array. Trino `IntArrayBlock`/`LongArrayBlock` wrap an on-heap
  `int[]`/`long[]`. Conversion collapses to **one bulk copy of the whole column buffer** into the
  primitive array + **one bulk copy of the validity bitmap** — ~memcpy bandwidth (GB/s), **one type
  dispatch per column per batch** instead of per cell. On little-endian hosts (all field pods) no
  byte-swap is needed. [PRIOR-ART] the Trino BigQuery Storage-API and Snowflake Arrow readers both
  convert Arrow batches **column-at-a-time** into Trino blocks; DuckDB and Velox import Arrow
  fixed-width buffers **zero-copy** (wrap, no copy) via the Arrow C-Data interface.
- **Variable-width (varchar/varbinary):** Trino's `VariableWidthBlock` is *literally* `(Slice
  values, int[] offsets, boolean[] valueIsNull)` — the **same three-buffer shape** as an Arrow
  `VarCharVector` (data buffer, offset buffer, validity). Build the block from **one `Slice`
  wrapping the Arrow data buffer** + **one offset-array copy** (int32→int32, or a widen for
  `LargeVarChar`), eliminating the ~3.9 M per-cell `byte[]` allocations entirely. This is the single
  biggest connector win for the narrow text shape.
- **Memory-lifetime caveat [CODE]/constraint A5/B4:** the Arrow data buffer is off-heap and owned by
  the Flight `VectorSchemaRoot`, freed when the stream advances/closes. A `Slice` *wrapping* it
  (true zero-copy) would dangle unless the block's lifetime is bounded to the batch — Trino pages can
  outlive a `getNextSourcePage` call. So the safe first step is **one bulk on-heap copy per column**
  (a `Slice.copyOf` of the whole data buffer), not a raw wrap: still ~memcpy-cheap, drops per-cell
  dispatch + per-cell alloc, and keeps the off-heap buffer's lifetime out of Trino's page graph
  (protects the worker heap budget, B4). Raw zero-copy wrap is a later, riskier step needing explicit
  retain/release plumbing.

**Estimated multiplier on the page-build step [DERIVED]:** row-by-row costs ~20–50 ns/cell
(fixed-width) and ~80–150 ns/cell (varchar, dominated by the `byte[]` alloc + young-gen pressure);
bulk column copy is ~1–3 ns/cell (memcpy-bound) + one dispatch/column/batch amortized to ~0. That is
**~10–20× on the page-build CPU** and **eliminates the 3.9 M allocs/scan**. Because page-build sits
on the serial pull chain (§1a), the *effective* stream multiplier is smaller than 10–20× (page-build
is one link of several) — call it **~1.5–3× on stream throughput [DERIVED]**, larger once prefetch
(below) also lands.

**Pair it with async prefetch [CODE] (distribution, not ceiling):** double-buffer the FlightStream —
pull batch *n+1* while converting batch *n* — so page-build CPU overlaps network/produce latency
instead of serializing behind it. Small change to `CqliteFlightPageSource`/`ReplicaFailoverStream`
(a one-slot read-ahead), directly attacks the §1a serialization.

---

## 3. Lever #3 — Arrow batch sizing (`--batch-size 8192`)

- **Current [CODE]:** server default `batch_size = 8192` rows (`main.rs:35–36`,
  `service.rs:238–243`, `producer.rs:401`). Batches are buffered to `batch_size` then emitted
  (`producer.rs:777,839`).
- **Bytes/batch on field rows [DERIVED]:** 8192 × ~180 B ≈ **1.47 MB/batch** (narrow keyvalue);
  a 1 KB wide row ⇒ ~8 MB/batch.
- **gRPC message ceiling:** 1.47 MB < gRPC's 4 MB default inbound cap, fine for narrow rows; but an
  **8 MB wide-row batch exceeds it** and would need a raised `maxInboundMessageSize` on the Java
  client (**not set today** — `FlightClient.builder(allocator, location).build()`,
  `CqliteFlightClient.java:101–103`, no options). Arrow-Flight-Java defaults the inbound limit high,
  so this is latent, not active, but it caps how far batch-size can grow for wide tables.
- **HTTP/2 flow-control interaction (the real issue) [DERIVED]:** a 1.47 MB batch is **~22× the
  default 64 KB HTTP/2 stream window** (§5). With an unconfigured window, the server can push only
  ~64 KB before blocking on a `WINDOW_UPDATE` round-trip → a large batch is drip-fed a window at a
  time. **Bigger batches don't help, and can hurt, until the window is opened** — batch-size and the
  window are coupled and must move together.
- **Verdict:** batch-size 8192 is reasonable for narrow rows; the lever here is **not** raising it in
  isolation but (a) making it **byte-bounded** for wide rows (cap MB/batch so one config works across
  shapes) and (b) opening the window (§5) so the batch can actually stream. Low standalone value;
  medium as a co-lever.

---

## 4. Lever #4 — IPC / Flight-body compression

- **Current [CODE]: OFF.** `encode_do_get` builds `FlightDataEncoderBuilder::new()` with **no
  `.with_options(...)`** (`streaming.rs:439–448`), so the Arrow-IPC body uses default
  `IpcWriteOptions` → **`batch_compression_type: None`**. Flight data bodies cross the app-node
  network **uncompressed**. (The `IpcWriteOptions::default()` at `service.rs:558` is GetSchema-only,
  also uncompressed, and schema is tiny — irrelevant.)
- **Wire savings [DERIVED]/[PRIOR-ART]:** Arrow LZ4_FRAME/ZSTD on columnar batches typically yields
  **~2–4×** on text/int columns (columnar data compresses well; the local Arrow wire form is already
  ~3.5× the packed on-disk size per Phase 0 §2, so there is fat to squeeze). ZSTD compresses harder,
  LZ4 is cheaper CPU.
- **CPU cost vs when it helps:** compression trades server CPU for wire bytes. It **helps when the
  link is the bottleneck** (cross-node network, or a window-throttled stream — §5) and **hurts when
  CPU is the bottleneck** (Phase 0: server is already ~55% kernel + 18% alloc for narrow rows —
  adding LZ4 there steals cycles from the merge). **At a genuine 250 MB/s app-node link, LZ4 (~500
  MB/s–1 GB/s/core encode) roughly breaks even to net-positive; ZSTD only pays if the network is the
  hard limit.** Decision rule: enable **LZ4 when the drain chain is network/window-bound** (wide
  rows, saturated NIC), leave **off when CPU-bound** (narrow rows on a fast intra-rack link).
  Make it a server flag, default off, opt-in per deployment. Confidence MED; needs a field A/B.
- **Constraint tie-in (B4 memory):** compression **reduces** peak wire buffering both sides — a mild
  win for the worker heap budget, independent of throughput.

---

## 5. Lever #5 — gRPC / HTTP-2 tuning

- **Server config today is one line [CODE]:** `main.rs:128–131` — `Server::builder()
  .max_concurrent_streams(max_concurrent_streams)` only, where `max_concurrent_streams = max(K×4,
  1024)` (`:101–104`). **No** `.initial_stream_window_size(...)`, **no**
  `.initial_connection_window_size(...)`, **no** `.max_frame_size(...)`. So the **HTTP/2 flow-control
  windows fall to tonic/h2 defaults (~64 KB stream, ~64 KB connection)** [PRIOR-ART: h2 crate
  defaults].
- **Client config today [CODE]:** `CqliteFlightClient.java:101–103` builds the FlightClient with
  **no** window / message / compression options — pure Arrow-Flight-Java defaults.
- **Why it bites [DERIVED]:** a 64 KB stream window against a 1.47 MB batch (§3) means a batch is
  released **~22 windows at a time**, each gated on a client `WINDOW_UPDATE`. On loopback (Phase 0)
  the round-trip is ~free, which is exactly why Phase 0 saw transport ≈0% and **could not see this**.
  On a real app-node link with RTT `r`, a single 64 KB-windowed stream tops out near `64 KB / r`
  (e.g. 0.5 ms ⇒ ~128 MB/s ceiling **per stream** regardless of NIC), and the *connection* window
  (also 64 KB, shared across all streams to a host) can throttle the aggregate even harder. This is a
  classic bandwidth-delay-product stall and a prime suspect for why per-stream field rates sit so far
  under the local ceiling.
- **Lever:** raise `initial_stream_window_size` and `initial_connection_window_size` to a BDP-sized
  value (e.g. 4–16 MB) on the tonic server, and match `maxInboundMessageSize` + window on the Java
  client. **Low code cost, potentially high payoff on the network-bound term**, and it is the
  *enabler* for larger/byte-bounded batches (§3) and for compression (§4) to matter. Confidence MED
  (the window default is real and unset in code; the field magnitude is unmeasured through Trino).
- **Constraint (B4/A5):** bigger windows raise per-connection buffered bytes on **both** heaps
  (server flight buffers + worker off-heap Arrow). Size the window against the worker's
  `≤512 Mi`/`≤16 Mi` B4 envelope and admission `K`, not blindly — a 16 MB window × many streams is a
  memory footgun. This is the load-bearing tension in the whole report: **every ceiling lever that
  raises in-flight bytes trades against B4.**

---

## 6. Lever #6 — Multiple streams per worker × egress-budget coupling (#2600/#2765)

- **The coupling the brief flags is real and bidirectional [FIELD]/[CODE].** #2600 (shipped, PR #2766)
  attributed R12's dominant saturation — **3,505 rows queued at merge-egress while blocking pool sat
  at 8/512** — to **consumer-side drain latency**: the Arrow-encode + gRPC-write stage (the egress
  channel's consumer) starved under CPU contention, so the merge's producer filled the channel. The
  lever shipped was a **process-global adaptive egress budget** `clamp(EGRESS_ROW_BUDGET /
  active_merges, MIN, 256)` (#2765 tracks the productionized impl), CAP=32 cutting depth 5–8× at <10%
  qps cost (MEMORY.md).
- **Why a worker-ingest fix feeds back into it [DERIVED]:** the egress channel's consumer chain is
  `encode → gRPC-write → [HTTP/2 window] → Java recv → page-build`. **Anything that speeds the Java
  drain (near-zero-copy page build §2, prefetch §2b, opened window §5) raises the real drain rate,
  which *empties* the egress channel** — reducing backpressure, letting more concurrent merges share
  the global budget, and letting `active_merges` climb toward the 64 admission ceiling. So the §2/§5
  levers and #2765 are **complements, not substitutes**: fix the drain and #2765's `clamp` stops
  binding as often; leave the drain slow and adding streams (§1a) just deepens the queue.
- **Distribution vs ceiling, stated cleanly:** *more streams* (raise `task.concurrency`/split count,
  async prefetch) is a **utilization** lever — it only helps once the **ceiling** (drain rate per
  stream) is high enough that the added streams don't just re-fill the egress queue. **Order matters:
  raise the ceiling (§2 page-build, §5 window, §4 compression-if-network-bound) first, then widen
  distribution (§1a, #2765 budget).** Doing distribution first re-creates the #2600 fire.

---

## 7. Lever table

Multiplier = expected effect on **pod rows/s** (≈ MB/s), not on the isolated sub-step. S/M/L = code cost.

| # | Lever | Multiplier (pod) | Cost | Risk | Type | Collisions / dependencies |
|---|---|---:|---|---|---|---|
| 1 | **Near-zero-copy `ArrowToTrino`** — bulk per-column copy; varchar via `VariableWidthBlock(Slice,offsets,nulls)`; kill 3.9 M/scan `byte[]` allocs | **1.5–3×** | **M** | MED — off-heap buffer lifetime vs Trino page graph (B4); use bulk on-heap copy first, raw wrap later | Ceiling | Epic AE #1470 (per-cell conversion, server-side sibling); Epic K #1604 (closed). **New surface — no existing connector issue owns it.** |
| 2 | **Async batch prefetch** in the page source (double-buffer `next()`/convert) | **1.3–2×** | **S** | LOW | Distribution | Pairs with #1; touches `CqliteFlightPageSource`/`ReplicaFailoverStream` |
| 3 | **HTTP/2 window sizing** (server `initial_stream/connection_window_size`, client match) | **1.5–3× (network-bound)** | **S** | MED — raises in-flight bytes both heaps (B4/A5) | Ceiling (enabler) | Enables #4/#5-batch; size vs B4 envelope + admission K |
| 4 | **Byte-bounded batch size** (cap MB/batch; raise client `maxInboundMessageSize`) | **1.0–1.3×** | **S** | LOW | Ceiling (co-lever) | Only pays **after** lever 3; #1476/#2230 (byte-budget, closed) are the read-path analogue |
| 5 | **Flight-body LZ4 compression, opt-in** (`FlightDataEncoderBuilder::with_options`) | **1.3–2× (network-bound); ≤1× (CPU-bound)** | **S** | MED — steals server CPU on narrow rows | Ceiling (conditional) | Default OFF; A/B in field; Phase 0 says server is CPU-bound for narrow rows |
| 6 | **Raise effective fan-out** — `task.concurrency`/split count toward admission 64 | **2–5× (only post-ceiling)** | **S** | HIGH if done first — re-creates #2600 egress fire | Distribution | **Gated on #2765** adaptive egress budget; ordering-critical (§6) |
| — | **Vectorized execution (DataFusion #941 / #2605)** | **≫ (Stage 2: 600 k rows/s)** | **L** | — | Ceiling (structural) | The only path past the row-engine ~1.6 µs/row wall (941 brief); out of this report's transport scope but the real Stage-2 answer |

**No duplicate filings:** per the inventory, levers 1–5 are **unclaimed transport/connector surface**
(the closest, AE #1470, is server-side per-cell cost, not connector page-build). Lever 6 must
**extend #2765**, not refile (inventory §Collision-watchlist: "extend #2765, don't refile"). Lever 3
must not be conflated with #2680/#2782 (an active split-scheduling P0 fire — a *different* connector
concern).

---

## 8. Is 250–350 MB/s/worker credible, and via what stack?

**Today: ~1.9 MB/s/worker [DERIVED].** The target is **~130–180× today** — far beyond the local
server-direct single-stream ceiling itself (~150 MB/s). **The answer is emphatically shape- and
stack-dependent:**

1. **Narrow rows (keyvalue, ~180 B) via the row engine + transport levers 1–6: NOT credible.**
   MB/s = rows/s × 180 B. To hit 250 MB/s you need **~1.4 M rows/s/worker** — *3× the entire local
   RF1/warm/uncompressed single-stream ceiling*, in the field, under RF3 + compression + network.
   The row engine's ~1.6 µs/row wall (941 brief) caps a single stream near ~600 k rows/s even
   perfectly tuned; you would need several such streams each at an unattainable field rate. The
   transport levers (1–6) realistically get narrow-row rows/s from ~10.6 k to **~40–120 k/pod**
   (compounding lever 1×2×3 ceiling gains ~3–6× and fan-out 2–5× once drain is fixed) — i.e.
   **~7–22 MB/s/worker**. Good (approaching the A4 Stage-1 100 k rung), but an order of magnitude
   short of 250 MB/s.

2. **Wide rows (≥1 KB) via row engine + levers 1–6: plausibly credible at the low end.**
   MB/s scales with row width at fixed rows/s. At ~100 k rows/s/pod (Stage-1-plausible post-levers)
   × 1 KB = **~100 MB/s/worker**; a 2–3 KB row at that rate reaches **200–300 MB/s** — so **250 MB/s
   is reachable for wide rows** on the row engine *if* fan-out and drain are fixed, because the
   per-row coordination overhead (Phase 0's dominant cost) amortizes over more bytes. Wide rows are
   the regime where levers 1 (bulk column copy) and 4 (compression) pay most.

3. **Narrow rows at 250–350 MB/s: only via vectorized/columnar execution (Stage 2/3, DataFusion
   #941).** 250 MB/s at 180 B/row = ~1.4 M rows/s — Stage-2/3 territory the 941 brief already
   concluded the row engine cannot reach. Transport levers are **necessary but not sufficient**; they
   remove the drain-chain ceilings so the columnar engine's output can actually leave the pod.

**Bottom line for the program:**

- **250–350 MB/s/worker is credible for wide rows on the tuned row engine**, and **not credible for
  narrow rows without vectorized execution.** The MB/s target implicitly assumes a wider row shape
  than the keyvalue benchmark; state the target's assumed row width explicitly.
- **The transport stack that gets there:** near-zero-copy `ArrowToTrino` (lever 1) + async prefetch
  (2) + opened HTTP/2 window (3) + byte-bounded batches (4) + fan-out-past-drain gated on #2765 (6),
  with LZ4 (5) toggled by whether the app-node link is the bottleneck. These are the **enabling
  layer**; DataFusion (#941/#2605) is the **rows/s engine** for the narrow-row Stage-2 target.
- **The binding constraint on every ceiling lever is B4 memory** (worker heap ≤512 Mi / ≤16 Mi
  per-page, plus server flight buffers): windows, batch bytes, and prefetch depth all raise in-flight
  bytes and must be co-sized against admission `K` and the B4 envelope. Size them together, not
  independently.
- **Ordering is load-bearing (§6):** raise the per-stream ceiling (1,2,3,4) *before* widening
  distribution (6), or you just rebuild the #2600 egress fire at a deeper queue.

---

## 9. Evidence index

| Claim | Location |
|---|---|
| Connector page build is row-by-row, per-cell `switch` dispatch | `trino-connector/.../ArrowToTrino.java:55–65,107–141` |
| Varchar path allocates+copies `byte[]` per cell (`VarCharVector.get(i)`) | `ArrowToTrino.java:307–334` + Arrow-Java `BaseVariableWidthVector.get` contract |
| Page source is synchronous pull, no prefetch/double-buffer | `CqliteFlightPageSource.java:45–64`; `ReplicaFailoverStream.java:67–121` |
| One split per token range, one replica pinned | `CqliteFlightSplitManager.java:288–337` |
| Aggregate path fans out serially (one do_get at a time) | `CqliteFlightAggregatePageSource.java:83–92` |
| Flight body compression OFF (default `IpcWriteOptions`) | `cqlite-flight/src/streaming.rs:439–448` |
| gRPC server sets only `max_concurrent_streams`; no window/frame tuning | `cqlite-flight/src/main.rs:101–104,128–131` |
| Client builds FlightClient with no window/message/compression options | `CqliteFlightClient.java:101–103` |
| batch_size default 8192 | `cqlite-flight/src/main.rs:35–36`; `service.rs:238–243`; `producer.rs:401` |
| admission K=64, blocking pool ~512 | `cqlite-flight/src/admission.rs:43`; `main.rs:42–44` |
| Server CPU: 55% kernel channel handoff, 18% alloc, transport ≈0% (loopback), decompression invisible | Phase 0 §3a, §3b, §5.1–5.3 |
| Field B3/R12: 1.94 M rows, 61.1 s, 3 pods = 10.6 k rows/s/pod; saturation admission 12/64, blocking 8/512, egress 3,505 | 941 decision brief (quoting #2367/#2600) |
| Row engine ~1.6 µs/row wall ⇒ Stage-2 needs vectorization | 941 decision brief |
| Adaptive egress budget (#2600 shipped / #2765 impl) | inventory §5; MEMORY.md #2600 |

**File:** `/Users/patrickmcfadin/local_projects/cqlite/docs/research/phase1-5-transport-ingest.md`
(uncommitted per instructions).
