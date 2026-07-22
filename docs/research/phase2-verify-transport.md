# Phase 2 — Adversarial verification of the transport/connector levers (T1–T6)

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Agent:** Phase-2 adversarial verifier (transport/connector)
**Target:** `docs/research/phase1-5-transport-ingest.md` (levers T1–T6)
**Calibration:** `docs/research/phase1-8-prior-art.md`, `docs/research/phase1-6-parallelism.md`, `docs/research/phase0-scan-cost-breakdown-2026-07.md`
**Mode:** READ-ONLY. No build was run. Every code claim below was re-checked at `file:line`; every version claim is read
off `Cargo.lock` / `trino-connector/build.gradle.kts`. API-contract claims (h2, grpc-java, Trino SPI, Arrow-Java) are
labelled **[API-CONTRACT]** and pinned to the resolved dependency versions.

---

## 0. Verdict summary

| # | Lever | Phase-1.5 claim | **Verdict** | Revised pod multiplier |
|---|-------|-----------------|-------------|------------------------|
| **T1** | Near-zero-copy `ArrowToTrino` page build | 10–20× page-build CPU; **1.5–3× stream throughput** | **WEAKENED (hard)** | **~1.0× today; ~1.1–1.2× only after the server per-stream ceiling is fixed.** Isolated-step 10–20× survives; the label "zero-copy" is wrong (a per-column copy is required) but the per-CELL→per-COLUMN distinction holds. |
| **T2** | Async Java-side prefetch (double-buffer) | 1.3–2× | **WEAKENED (hard)** | **~1.0× today.** Fills stream idle time that is ~99.9% server-produce wait at field rates. Compounds with T1 post-fix; combined T1+T2 ≤ ~1.1–1.3×, not the doc's 2–6× product. |
| **T3** | HTTP/2 window sizing ("1.47 MB batch = 22× the 64 KB window → BDP stall") | 1.5–3× (network-bound) | **KILLED** | **~1.0×.** Double kill: (a) the binding receive window is the **grpc-java client's, defaulting to 1 MiB with BDP auto-tuning**, not 64 KB; the tonic *server* window knob the doc names is the wrong flow-control direction for `do_get` egress. (b) Even a 64 KB window has ~8× headroom over the actual 0.78 MB/s per-stream field byte rate. |
| **T4** | Byte-bounded batch sizing | 1.0–1.3× | **SURVIVES (minor)** | **~1.0–1.1×.** Real robustness win for wide rows; but the gRPC message-ceiling motivation is moot (Flight defaults `maxInboundMessageSize` high). Keep as a correctness/robustness lever, not a throughput one. |
| **T5** | Opt-in Flight LZ4/ZSTD body compression | 1.3–2× (net-bound); ≤1× (CPU-bound) | **WEAKENED → conditional** | **~1.0× today; net-NEGATIVE for narrow rows.** Since T3 shows the link is *not* binding at current per-stream rates, compression's throughput case is empty today. Survives only as a latent WAN lever + a mild egress-buffer (B4) reducer. |
| **T6** | Fan-out past drain (gated on #2765) | 2–5× (post-ceiling only) | **SURVIVES (as stated)** | Correctly gated and correctly ordered *last*. This verification reinforces: it must not precede the server-side ceiling fix. |

**One-line program consequence:** the Phase-1.5 transport levers are aimed at the **last two links of the drain chain
(network + Java page build), which are idle ~99.9% of the per-batch cycle at field rates.** The binding constraint is
**server-side produce/drain** (cold IO + LZ4 decompress + the single-threaded merge coordinator + the per-row
`sync_channel`, and the N_drain_sat≈8 egress saturation) — none of which any of T1–T5 touch. Ordering is not a nuance
here; it is the whole finding.

---

## 1. Code + version claims re-verified (all TRUE)

| Claim | Location | Status |
|---|---|---|
| `toPage`→`toBlock`→`writeValue` is a row-at-a-time per-cell `switch` dispatch | `ArrowToTrino.java:107–141` | **CONFIRMED** — `for (i<rowCount) { isNull(i); writeValue(...) }`; `switch(type)` re-decided every cell (`:123–140`). |
| Varchar path calls `VarCharVector.get(i)` (per-cell `byte[]` alloc+copy), then `Slices.wrappedBuffer` | `ArrowToTrino.java:319–334` | **CONFIRMED, with a correction** — `Slices.wrappedBuffer(v.get(i))`: `v.get(i)` allocates+copies one `byte[]`; `wrappedBuffer` then **wraps (no second copy)**. So it is **1 alloc + 1 copy per varchar cell**, not "2 copies." The doc's "3.9 M `byte[]` allocs/scan" (allocations) is right; do not restate it as copy-bandwidth. |
| Page source is a synchronous pull, no prefetch/double-buffer | `CqliteFlightPageSource.java:45–64`; `ReplicaFailoverStream.java:67–121` | **CONFIRMED** — `stream.next()` (blocks) → `toPage` → return, on the calling thread; the only `ExecutorService`/`CompletableFuture` in the connector is `SnapshotManager` (planning-time), not the drain path. |
| Aggregate path fans out serially | `CqliteFlightAggregatePageSource.java:83–92` | CONFIRMED (off the B3 critical path; scalar scan governs). |
| Flight body compression OFF | `streaming.rs:439–448` — `FlightDataEncoderBuilder::new().with_schema(...).build(...)`, no `.with_options(...)` | **CONFIRMED** → default `IpcWriteOptions` → `batch_compression_type: None`. The other `IpcWriteOptions::default()` (`service.rs:558`) is GetSchema-only. |
| gRPC server sets only `max_concurrent_streams`; no window/frame tuning | `main.rs:101–104,128–131` | **CONFIRMED** — `Server::builder().max_concurrent_streams(...)` only; no `initial_stream_window_size`/`initial_connection_window_size`/`max_frame_size`. |
| Java client builds FlightClient with no window/message/compression options | `CqliteFlightClient.java:101–103` | **CONFIRMED** — `FlightClient.builder(allocator, Location.forGrpcInsecure(...)).build()`; inherits Flight/grpc-java defaults. |
| batch_size default 8192 | `main.rs:35–36` | CONFIRMED. |

**Resolved versions (load-bearing for the API-contract rulings):**
- **Rust server transport:** `cqlite-flight` → `arrow-flight 53.4.1` → **`tonic 0.12.3`** → **`h2 0.4.15`**, `hyper 1.10.0` (from `Cargo.lock`).
- **Java connector transport:** `trino-connector/build.gradle.kts` → **Trino 481**, **arrow-java 19.0.0** (`flight-core:19.0.0`) → **`grpc-netty:1.79.0`**, **netty 4.1.130.Final**.

---

## 2. THE T3 AUTO-TUNE RULING (KILLED)

The T3 claim rests on one premise: *the binding HTTP/2 window is 64 KB, so a 1.47 MB batch stalls at ~22 windows/batch,
BDP-throttling each stream.* Two independent facts falsify it.

### 2a. Flow-control direction: the tonic-server window knob is the wrong direction for `do_get`
HTTP/2 flow control is **receiver-advertised and per-direction**. In a `do_get`, bulk data flows **server → client**. The
window that limits how fast the tonic server may *send* is the window the **Java client advertises for receiving** — plus
the client's `WINDOW_UPDATE`s. The tonic server's `initial_stream_window_size` / `initial_connection_window_size`
(the knobs Phase 1.5 §5 says to raise) govern data the **server receives** (client → server), which for `do_get` is only
the tiny Ticket. **[API-CONTRACT, h2 0.4.15]** `h2::server::Builder::initial_window_size` is documented as the window "for
**received** data." **Raising it does nothing for `do_get` egress.** h2 on the send side needs no configuration — it emits
DATA up to whatever the peer's window allows and honors incoming `WINDOW_UPDATE`s automatically.

### 2b. The client window is not 64 KB — it is 1 MiB with BDP auto-tuning
**[API-CONTRACT, grpc-java 1.79.0]** grpc-netty's `DEFAULT_FLOW_CONTROL_WINDOW = 1048576` (**1 MiB, 16× the assumed
64 KB**), and grpc-java performs **BDP-based flow-control auto-tuning** (the `FlowControlPinger` / BDP-ping path) that grows
the connection window from that floor toward the measured bandwidth-delay product. Auto-tuning is **on by default** and is
disabled only when the application *pins* `flowControlWindow(int)`. Arrow Flight's `FlightClient.builder(...).build()`
(`CqliteFlightClient.java:101–103`) does **not** pin it, so the connector's client runs at the **1 MiB floor with
auto-tuning enabled**. So the receive window that actually governs `do_get` is ≥ 1 MiB and adaptive — the "64 KB → 22×"
arithmetic is against a window that does not exist in this stack.

The Rust `h2` side, for completeness, does **not** auto-tune (fixed 64 KB default, `initial_window_size` unset) — but per
2a that default is on the irrelevant direction, so its non-auto-tuning is moot.

### 2c. Even the strawman 64 KB would not bind at field per-stream rates
Field per-stream throughput (Phase 1.5 §1a): 2.6 k rows/s × ~300 B/Arrow-row (Phase 1.8: 150 MB/s ÷ 500 k) ≈ **0.78 MB/s
per stream**. A single 64 KB window tops a stream out near `64 KB / RTT`: **6.4 MB/s at a WAN-ish 10 ms**, 64–320 MB/s
intra-rack. Actual demand 0.78 MB/s has **~8× headroom even against the pessimistic 64 KB / 10 ms ceiling.** The window is
not the limiter; the **server produce rate** is. The window only becomes binding after the per-stream produce rate rises
~10× **and** the deployment is high-RTT — both false today.

**T3 ruling: KILLED as a near-term lever.** Reclassify as *latent, client-side, WAN-contingent*: if a future high-RTT
deployment appears **and** the per-stream produce ceiling has already risen ~10×, the fix is to raise/confirm the
**grpc-java client** window (or leave auto-tuning to do it) — **not** to set `initial_stream_window_size` on the tonic
server. Revised multiplier **~1.0×** in every current field scenario.

---

## 3. THE T1 SPI-COPY RULING (WEAKENED; distinction holds, label is wrong)

Two sub-questions from the attack brief.

### 3a. Does `VarCharVector → VariableWidthBlock` force a copy? Yes — for *lifetime*, not for SPI/arena reasons.
**[API-CONTRACT, Trino 481 SPI]** `io.trino.spi.block.VariableWidthBlock` has a public constructor
`VariableWidthBlock(int positionCount, Slice slice, int[] offsets, Optional<boolean[]> valueIsNull)` that **retains the
Slice by reference — it does not copy into a Trino arena.** There is **no** `AggregatedMemoryContext` reservation required
to *construct* a Block: a scan `ConnectorPageSource` returns Pages and Trino accounts their `getRetainedSizeInBytes()`
downstream; it is not obliged to pre-reserve from a memory pool the way a hash-build operator is. So **Trino's SPI does not
forbid building a block from a foreign Slice**, and the current `getMemoryUsage()` returning 0 (`CqliteFlightPageSource.java:82`)
is consistent with that.

The forcing function is **buffer lifetime**, not accounting. The Arrow data buffer is off-heap, owned by the Flight
`VectorSchemaRoot`, and **freed when `FlightStream.next()` advances or the stream closes.** A Trino Page/Block can be
retained by operators (exchange buffers, hash tables) **long after** the `getNextSourcePage()` that produced it. A Slice
*wrapping* the Arrow off-heap buffer would therefore be a **use-after-free**; a true zero-copy wrap needs explicit
Arrow refcount retain/release plumbing tied to Block lifetime, for which Trino's Block lifecycle exposes no hook.

### 3b. Does the per-CELL → per-COLUMN distinction survive the forced copy? YES.
The safe implementation is a **bulk on-heap copy per column**:
- **varchar:** `Slice.copyOf` the entire Arrow data buffer once (one memcpy/column/batch); the Arrow int32 offset buffer is
  **directly reusable** — Arrow's cumulative-byte-offset semantics equal Trino `VariableWidthBlock`'s `int[] offsets`; copy
  the validity into a `boolean[]`. This collapses **N per-cell `byte[]` allocations → 1 bulk copy** and **N per-cell
  `switch` dispatches → 1**.
- **fixed-width:** one bulk copy of the data buffer into the `int[]`/`long[]` backing an `IntArrayBlock`/`LongArrayBlock`
  + one validity copy; little-endian on all field pods, no byte-swap.

**So T1's load-bearing claim (per-cell → per-column) HOLDS.** What does **not** hold is the *name*: this is **not**
"near-zero-copy," it is a **bulk per-column copy**. The same string bytes are still copied off-heap→heap; the win is
**allocation count + dispatch elimination**, not copy-bandwidth reduction. Phase 1.5's own §2b memory-lifetime caveat and
its lever-table risk note already say exactly this (bulk on-heap copy first, raw wrap "a later, riskier step") — the doc is
internally honest; only the lever headline oversells. **The isolated page-build 10–20× is defensible** for the
alloc-dominated narrow-text shape.

### 3c. But the POD multiplier (1.5–3×) does NOT survive the ordering math.
At field rates a stream delivers 2.6 k rows/s = one 8192-row batch **every ~3.15 s**. A pessimistic page-build (100 ns/cell
× 8192 × 2 cols) ≈ **1.6 ms**, i.e. **~0.05 % of the per-batch cycle**; the other ~99.95 % is the Java thread **blocked in
`stream.next()` waiting on the server** (cold IO + LZ4 decompress + single-threaded coordinator). Eliminating page-build
entirely buys ~0.05 % at the pod. Phase 1.8 §3c states this outright: *"the CQLite producer, not the JVM page builder, is
the real limiter."* **T1's 1.5–3× pod multiplier is not defensible at current field per-stream rates.** It materializes
only *after* the server per-stream ceiling is raised ~100× (toward the ~500 k local ceiling), at which point a batch
arrives every ~16 ms and page-build's ~1.6 ms is ~10 % of the cycle → T1 (+ prefetch) is worth **~1.1–1.2×**. Revised:
**~1.0× today, ~1.1–1.2× post-server-fix.**

---

## 4. THE ORDERING RULING (worker-side vs server-drain) — the headline

**Server-side first, decisively. Worker-side T1/T2 buy ≈0 until the server produce/drain ceiling is raised ~100×.**

The drain chain (Phase 1.5 §0):
```
SSTable → decode → k-way merge → egress channel(#2600) → Arrow encode → gRPC write
  → [network + HTTP/2] → FlightStream.next() → ArrowToTrino page build → Trino Page
```
Three independent Phase-0/1 measurements converge on **where the time is, and it is not the last two links**:
1. **Phase 0:** server single-stream CPU is 55 % kernel `sync_channel` park/wake + 18 % alloc + 32 % coordinator merge
   compute; Arrow encode 1 %, transport ≈0 %. The **single-threaded coordinator + per-row channel** is the per-stream
   ceiling.
2. **Phase 1.6 §4:** throughput saturates at **N_drain_sat ≈ 8 concurrent streams** — a **server-side** drain limit
   (Arrow-encode + gRPC-write + coordinator starving under contention), the #2600/#2765 egress phenomenon. Past 8, only
   latency and buffer depth grow.
3. **Phase 1.8 §3c:** the JVM page builder is explicitly **not** the binding constraint.

**Attack-line-3 (double-counting) resolved:** T1 (labelled "ceiling") and T2 (labelled "distribution") both attack the
worker-side drain, and in the current synchronous-pull design page-build *is* serialized into stream idle time — but that
idle time is ~99.9% **server-produce wait**, so filling it (T2) and cheapening it (T1) both fish in a ~0.05 % pool.
Multiplying their separate multipliers (1.5–3× × 1.3–2× = 2–6×) **double-counts the same idle-fill headroom**. Their
honest **combined** pod ceiling is bounded by page-build's fraction of the per-batch cycle: **≤ ~1.1–1.3×, and only after
the server ceiling is fixed.**

**Correct order of operations:**
1. **Server merge/produce ceiling** — Phase-0 #1 (batch the per-row `sync_channel` handoff / inline merge for the
   few-SSTable case) + #2 (reconcile compute). **Out of transport scope**, but it is the gate everything else waits behind.
2. **Server-side data-plane** — cold-IO residency + LZ4 decompression (read-path epics). Out of transport scope; likely the
   single biggest field term (Phase 1.5 §1b, LOW confidence, 2–8×).
3. **Server-side drain** — #2600/#2765 adaptive egress budget (raises N_drain_sat).
4. **THEN worker-side T1 + T2 together (~1.1–1.3×)** become visible, because only then is page-build a non-trivial fraction
   of the cycle.
5. **T6 fan-out** last, gated on #2765 — Phase 1.5/1.6 already order this correctly; doing it before step 1/3 rebuilds the
   #2600 egress fire at a deeper queue.

Transport levers are **enabling, not driving**: they remove ceilings so a *future* fast producer's output can leave the
pod. On today's slow producer they are premature.

---

## 5. Attack-line 4 — the JDBC 32 MB/s figure (no miscitation to kill)
Phase 1.5 does **not** lean on the JDBC 32 MB/s figure for any lever, and Phase 1.8 §3c handles it correctly: the CQLite
connector consumes **Arrow record batches** (`ArrowToTrino.toPage` builds Trino Pages from Arrow vectors), **not** JDBC
`ResultSet` rows. The Starburst ~32 MB/s/conn ceiling is a **row-by-row JDBC deserialization** number and is
**categorically inapplicable** to this path — the relevant envelope is the Arrow-batch class (GB/s localhost; Phase 1.8
§1). Any future claim that cites 32 MB/s as a CQLite *worker-ingest* limit would be a category error; neither target doc
commits it. **Ruling: correctly excluded; nothing to kill, but flag the category boundary so it is not reintroduced.**

---

## 6. Attack-line 5 — B4 memory co-sizing (closes, but only at the correct operating point)
Per-stream buffered bytes, both heaps:

| Buffer | Owner | Bytes/stream | Source |
|---|---|---|---|
| Arrow egress (`(DO_GET 4 + IN_FLIGHT 3) × 8192` = 57,344 rows) | server | **~15–20 MB** (narrow ~300 B/row) | Phase 1.6 §6.1 (`streaming.rs:65/86`) |
| Merge fan-in (`256 × M` MergeEntry) | server | ~0.5–2 MB (M≤8) | Phase 1.6 §6.1 |
| grpc-netty flow-control receive buffer | worker (off-heap, netty direct) | **1 MiB floor, auto-tuning toward BDP** | grpc-java 1.79.0 default |
| Trino Blocks for one batch | worker (on-heap) | ~batch × row-width, bounded to one batch | `ArrowToTrino` |

- **The transport levers' memory impact is modest at the correct width (N_drain_sat ≈ 8, admission re-sized to ~16 per
  Phase 1.6 §6.3):** server ≈ 8 × ~20 MB ≈ 160 MB flight buffers (comfortable under 512 Mi); worker ≈ 8 × (1–8 MiB netty +
  one on-heap batch) — tens of MB. **The story closes.**
- **It does NOT close if T3's window is blindly raised to 4–16 MB × a fanned-out K.** But T3 is killed anyway (the client
  window is already 1 MiB auto-tuning; the tonic-server knob is the wrong direction), so this footgun never needs to be
  loaded. The one live memory caveat is that grpc-java **auto-tuning can grow the worker's netty direct-memory window**
  (toward ~8 MiB × K) independent of any code change — worth watching in the worker's off-heap budget, but it is
  netty/grpc-managed direct memory, **separate from Trino's 512 Mi heap accounting**, and bounded by BDP (small at low
  per-stream rates).
- **T1's bulk-copy** adds one transient per-column on-heap copy (one batch's worth), bounded — no B4 hazard.
- **T4 (byte-bounded batch)** is the one transport lever that *helps* B4: it caps MB/batch for wide rows, which otherwise
  scales the 57,344-row egress buffer to ~8 MB/batch × depth.

**Ruling: the co-sizing closes under the correct operating point (K≈8, admission ≈16, no blind window inflation).** It does
not close under the naive "raise everything" reading — but the only lever that would have forced that (T3 server window) is
killed.

---

## 7. Revised program guidance

1. **Do not schedule T1/T2/T3/T5 as throughput work now.** At field per-stream rates the network and Java page-build are
   idle-adjacent (≥99.9 % of the cycle is server-produce wait). Their measured field payoff today is ~1.0×.
2. **The transport lever with a real, immediate, non-throughput case is T4** (byte-bounded batch, wide-row robustness +
   B4) — cheap, safe, worth doing on its own merits, **~1.0–1.1×**.
3. **T1 keeps its engineering value but must be reframed:** it is a **bulk per-column copy** (not zero-copy), worth
   **~1.1–1.2× at the pod only after** the server produce ceiling is raised, and it **must ship with T2** (prefetch) so its
   cheaper page-build can overlap the network — the two are complements whose *combined* ceiling is ≤ ~1.3×, not a product.
4. **T3 should be closed as "not applicable in this stack"** with the flow-control-direction + grpc-java-auto-tuning
   findings recorded, so it is not refiled. If a WAN deployment ever appears, the knob is the **grpc-java client** window,
   not `initial_stream_window_size` on tonic.
5. **T5 stays opt-in/off and explicitly net-negative for narrow rows** (steals server CPU from the coordinator that is the
   bottleneck); revisit only if a field A/B ever shows a genuinely network-bound link — which T3's headroom analysis says
   does not exist today.
6. **T6 ordering is correct as written** and this verification hardens it: fan-out is last, after the server ceiling and
   #2765.
7. **The real levers live upstream of this report:** Phase-0 #1 (per-row channel batching / inline merge), the read-path
   cold-IO/decompression epics, and #2600/#2765 egress. Transport is the enabling layer for a fast producer that does not
   yet exist.

---

## 8. Evidence index (this report)

| Ruling | Basis |
|---|---|
| T3 wrong-direction | `h2 0.4.15` `server::Builder::initial_window_size` = received-data window; `do_get` data is server→client (`main.rs:128–131`, `streaming.rs:439–448`) |
| T3 client window 1 MiB + auto-tune | grpc-java 1.79.0 `DEFAULT_FLOW_CONTROL_WINDOW=1048576` + BDP `FlowControlPinger`; not pinned by Flight (`CqliteFlightClient.java:101–103`); resolved via `build.gradle.kts` (arrow-java 19.0.0 → grpc-netty:1.79.0) |
| T3 headroom | 2.6 k rows/s × 300 B = 0.78 MB/s vs 64 KB/10 ms = 6.4 MB/s (Phase 1.5 §1a, Phase 1.8 §1) |
| T1 SPI accepts foreign Slice | Trino 481 `VariableWidthBlock(int, Slice, int[], Optional<boolean[]>)`; no arena reservation for scan Pages; `getMemoryUsage()=0` (`CqliteFlightPageSource.java:82`) |
| T1 copy forced by lifetime | Arrow off-heap buffer freed on `FlightStream.next()` advance; Trino Blocks outlive `getNextSourcePage` |
| T1 per-column distinction | Arrow int32 offsets == Trino `VariableWidthBlock` offsets; one `Slice.copyOf`/column (`ArrowToTrino.java:319–334`) |
| T1 pod multiplier ≈0 today | 8192 rows / 2.6 k rows/s = 3.15 s/batch; page-build ~1.6 ms = 0.05 % (Phase 1.8 §3c) |
| Ordering: server is limiter | Phase 0 §3a/§3b; Phase 1.6 §4 (N_drain_sat≈8, server-side); Phase 1.8 §3c |
| Sync pull confirmed | `CqliteFlightPageSource.java:45–64`; `ReplicaFailoverStream.java:67–121` |
| Compression OFF confirmed | `streaming.rs:439–448` (no `.with_options`) |
| Versions | `Cargo.lock` (tonic 0.12.3 / h2 0.4.15); `build.gradle.kts` (Trino 481, arrow-java 19.0.0, grpc-netty 1.79.0, netty 4.1.130.Final) |

**File:** `/Users/patrickmcfadin/local_projects/cqlite/docs/research/phase2-verify-transport.md` (uncommitted per instructions).
