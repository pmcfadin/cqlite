# Design — Flight do_get admission control (issue #2420, WS4)

## Context

`do_get` today runs unbounded. The merge runs on `spawn_blocking`
(`streaming.rs:317`) after an eager setup `spawn_blocking` (`service.rs:634`); each
scan opens a fresh fd per SSTable and streams batches through a bounded channel
(`DO_GET_CHANNEL_CAPACITY = 4` + allowance). The crate already has cancel-aware
teardown: `CancelFlag` (a `CancellationToken` + a synchronous `ScanCancel`,
`cancel.rs`), `CancelGuard` (cancel-on-drop, disarm-on-success), and an async
`cancelled()` future the streaming sink races against a full-channel send
(#2264/#2383). `RpcMetrics` maintains the `cqlite.rpc.in_flight` gauge as an
up/down level; `PhaseTimer` maintains per-phase in-flight counters. There is no
Semaphore and no transport concurrency limit.

The connector's failover machinery (`ReplicaFailoverStream`, `ReplicaFailover`,
#2241) is **decisive** for the semantics choice and is quoted in §(b).

The product decisions (a)–(f) below each state the alternatives; §Recommended
package selects one coherent set for Seam-1 approval.

---

## (a) Admission mechanism: tonic `concurrency_limit` vs. owned `Semaphore` vs. both

- **tonic `concurrency_limit` / `max_concurrent_streams` (transport-wide).** One
  line on `Server::builder()`. But it is **RPC-agnostic** — it would throttle
  `handshake`, `get_flight_info`, `do_action` identically to `do_get`, even
  though only the merge is the heavy consumer — and it is **opaque**: no
  cancel-aware release semantics we control, no per-scan gauge, no wait/reject
  distinction we can observe or shape. Queued requests park inside tonic with no
  signal we emit.
- **Owned `Semaphore` in `do_get` (scan-scoped).** `K` permits, acquired at the
  top of `do_get_inner` before setup opens anything, held by an RAII guard for the
  scan lifetime, released on completion/drop/cancel. Gates **exactly** the merge,
  is directly observable, and releases the permit the moment the `CancelGuard`
  fires — the precise lifecycle §(d) needs. Precedent exists in-tree:
  `cqlite-core/.../sstable_data_manager.rs:183` gates the write-engine data
  manager with an `operation_semaphore` (a different subsystem — see §(f)).
- **Both, layered.** The Semaphore is the real, observable, cancel-aware admission
  ceiling; a generous tonic `max_concurrent_streams` (well above `K`) is a coarse
  backstop protecting the accept loop / HTTP-2 stream table from a client that
  opens far more streams than `K`.

**Recommendation: both, Semaphore-primary.** The owned Semaphore does the real
admission work (scan-scoped, observable, cancel-releasable); the tonic cap is a
coarse transport guard, not the throttle. Using tonic *alone* fails §(d) (no
cancel-aware release) and §(e) (no admission observability) and mis-throttles
lightweight RPCs.

## (b) Overload semantics: hard reject vs. bounded queue-and-wait — the client contract

The connector contract makes this consequential. `ReplicaFailoverStream.next()`
(trino-connector) only fails over to the next replica when **both**: (i) no batch
of this stream has been delivered yet (`started == false`), and (ii)
`ReplicaFailover.isConnectClass(e)` is true — which is **`FlightStatusCode.UNAVAILABLE`
only**. Any other status (including `RESOURCE_EXHAUSTED`) is rethrown → the split
fails → the query fails.

Consequences for each option:

- **Hard reject with `RESOURCE_EXHAUSTED`.** The connector does **not** retry or
  fail over — a saturated server turns offered load directly into **query
  failures**. Making it retryable would require a connector change (a new
  admission-aware retry path). Worst client experience with no connector work.
- **Hard reject with `UNAVAILABLE`.** The connector **fails over to the next
  replica in the split's ordered list** (retry-safe: no batch delivered yet, so
  no row duplication). This composes with the existing #2241 machinery *today*,
  with no connector change. The mild cost is semantic overload of `UNAVAILABLE`
  ("endpoint down") to mean "endpoint busy"; under *global* saturation every
  replica rejects and the client eventually sees the last failure propagate —
  loud, never silent.
- **Bounded queue-and-wait (timeout).** The request parks for a permit up to a
  configured timeout, absorbing short bursts **transparently** — no status, no
  connector change, the split just takes longer. Only on timeout must a status be
  returned; per the contract above that status should be `UNAVAILABLE` so a
  sustained-overload reject sheds to a less-loaded replica rather than failing the
  query.

**Recommendation: bounded queue-and-wait, and on timeout return `UNAVAILABLE`
(never `RESOURCE_EXHAUSTED`).** Rationale: (1) short bursts are absorbed with zero
client-visible error and zero connector change; (2) sustained overload sheds to
another replica via the existing #2241 failover (correctness-safe because the
reject provably precedes the first batch); (3) only when *every* replica is
saturated does the query fail loudly — the graceful-degradation ladder the epic
asks for. Choosing `RESOURCE_EXHAUSTED` would convert saturation into query
failures and is rejected on that basis; a connector-side admission-retry is a
possible future refinement but is out of scope here (no Trino-side change).

## (c) Limit sizing: fixed default vs. f(cores) vs. config knob

- **f(num_cpus) alone.** CPU is **not** the binding constraint. The Rank 2/3/4
  failures are blocking-pool threads (512 cap, ~2 per scan → ~256 ceiling), fds
  (~1024 ulimit ÷ M SSTables), and RSS — none of which scale with core count.
  A cores-derived default would be miscalibrated.
- **Fixed default.** Simple and predictable, but a single baked-in number cannot
  fit every deployment's ulimit / SSTable-count / memory envelope.
- **Config knob with an evidence-based default.** `--max-concurrent-scans` (CLI
  flag + env), default a conservative fixed value sized against the
  blocking-pool ceiling (~256) and fd ceiling (~1024 ÷ M) — well below both — and
  **validated by the WS1 ramp (WS8)** before the default is locked. Per the AH
  decorative-knob doctrine, the knob is *real*: a test proves that setting `K`
  bounds in-flight scans to `K` end-to-end.

**Recommendation: a real config knob, default a conservative fixed value pending
WS1-ramp validation, sized from the blocking-pool/fd ceilings, NOT from
num_cpus.** The permit-wait timeout §(b) is a second (injectable) knob. Both are
wired end-to-end and covered by tests; neither is decorative.

## (d) Permit lifecycle under cancellation

A cancelled or superseded `do_get` **must** release its permit promptly — a leaked
permit permanently lowers `K` and eventually deadlocks admission.

- The permit is held by an **RAII guard dropped on every exit path** of the scan:
  normal completion, setup error, client disconnect (response-stream drop), and
  cooperative cancellation. The guard is owned by the same structure that owns the
  `CancelGuard` today (the metered stream / merge task), so a future-drop that
  already fires `CancelFlag` **also** drops the permit — one lifetime, no separate
  bookkeeping.
- Because the permit is `Owning` (`OwnedSemaphorePermit`) and moved into the
  stream, dropping the stream (the #2264/#2383 disconnect path) frees it without
  any explicit `release` call — leaks are structurally impossible on the drop
  path. The acquire happens *before* setup, so a request cancelled while waiting
  for a permit simply abandons the `acquire` future and never held one.

**Requirement:** a saturation test asserts the admission gauge returns to its
pre-scan level after all admitted scans are cancelled/dropped (zero permit leak).

## (e) Observability

New catalog instruments (in `cqlite-core` `observability::catalog`, recorded from
`cqlite-flight/src/obs.rs`), composing with WS2 (#2419) rather than duplicating
`cqlite.rpc.in_flight`:

- `cqlite.flight.admission.limit` (gauge) — configured `K`.
- `cqlite.flight.admission.in_use` (gauge, up/down level like RPC_IN_FLIGHT) —
  permits currently held.
- `cqlite.flight.admission.waiting` (gauge) — requests parked on `acquire`.
- `cqlite.flight.admission.rejected_total` (counter) — timeout rejections.
- `cqlite.flight.admission.wait_seconds` (histogram) — permit-acquire latency.

These are admission-specific and distinct from the RPC-level in-flight gauge (that
counts *accepted* RPCs including the queued ones; `in_use` counts *admitted*
scans). The counters are scale-free (levels + monotonic totals), so tests assert
them without fixture-size coupling.

## (f) Interaction with the machine's blocking-pool admission (#1594 class)

Two distinct layers, both needed:

- **`sstable_data_manager`'s `operation_semaphore`** (#1594 class,
  `cqlite-core/.../sstable_data_manager.rs:228`) bounds concurrent operations
  *inside the write-engine data manager* — a core-storage concern, per-manager,
  unrelated to the Flight transport.
- **This WS4 admission Semaphore** bounds concurrent *`do_get` transport scans* at
  the server boundary, before any core-storage work begins.

They are not redundant: WS4 caps how many scans *enter* the server; the data
manager caps concurrency *within* a storage subsystem a scan may touch. WS4 sits
strictly above and does not re-count the data-manager permits (a `do_get` merge is
read-path and does not acquire the write-engine data-manager permit at all). The
design keeps them independent knobs so neither silently constrains the other.

---

## Recommended package (for Seam-1 approval)

1. **(a) Owned `Semaphore` primary + coarse tonic `max_concurrent_streams`
   backstop.** Scan-scoped, observable, cancel-releasable admission; transport cap
   guards the accept loop only.
2. **(b) Bounded queue-and-wait; on timeout reject with `UNAVAILABLE`.** Absorbs
   bursts transparently, sheds sustained overload to another replica via existing
   #2241 failover (retry-safe pre-first-batch), fails loudly only when all
   replicas saturate. Never `RESOURCE_EXHAUSTED` (the connector would fail the
   query).
3. **(c) Real `--max-concurrent-scans` knob (CLI + env) + injectable permit-wait
   timeout; default a conservative fixed value validated by the WS1 ramp, sized
   from blocking-pool/fd ceilings, not num_cpus.** Anti-decorative: tested to
   truly bound in-flight to `K`.
4. **(d) `OwnedSemaphorePermit` held by the stream/merge-task RAII guard alongside
   the `CancelGuard`** — released on every exit path incl. disconnect/cancel; zero
   leak asserted.
5. **(e) Five admission instruments** composing with WS2, distinct from
   `cqlite.rpc.in_flight`.
6. **(f) Independent of the #1594 data-manager semaphore** — two layers, WS4 above
   the transport boundary.

## Alternatives considered and rejected

- **Transport `concurrency_limit` only** — fails §(d)/(e), mis-throttles
  lightweight RPCs.
- **Hard `RESOURCE_EXHAUSTED` reject** — connector fails the query (no #2241
  failover); would need Trino-side work to be graceful.
- **f(num_cpus) default** — mis-sized against the real (fd/blocking-pool)
  constraints.
- **Unbounded wait (no timeout)** — under sustained overload the client sees a
  hang instead of a sheddable status; violates the "backpressure signal" goal.

## Testing strategy (determinism)

- **Injectable concurrency, no wall-clock.** Tests hold `K` permits via a test
  barrier / pre-acquired guards, then offer `K + M` requests; assert admitted
  in-flight ≤ `K` and the `M` excess either wait (bounded) or reject with
  `UNAVAILABLE` per the injected timeout. The timeout is a configured value, not a
  real sleep; any observation window is captured to cover **all** sampled requests
  (pre-roborev wall-clock-race checklist).
- **Scale-free counters.** Assertions read the admission gauges/counters as
  levels + totals, independent of fixture row/SSTable count.
- **Wiring through the flight surface.** The saturation test drives real `do_get`
  RPCs (or the `do_get_inner` surface) end-to-end, not a helper in isolation —
  proving the ceiling engages on the public path.
