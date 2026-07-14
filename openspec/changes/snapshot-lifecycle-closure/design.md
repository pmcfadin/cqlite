# Design — Snapshot lifecycle closure

## Context

The Trino connector's default read mode is per-query snapshot (issue #2105/#2227). Per query,
`SnapshotManager.snapshotFor(queryId, ks, table, hosts)` creates a snapshot named `cqlite-<queryId>` on
EVERY replica host's Sidecar (instance-local PUT, #2227), the splits carry that name in their flight
tickets, and `cleanup(queryId)` best-effort deletes it at query end (a Sidecar TTL is the leak
backstop). On the Rust side, `FlightService::resolve_dir` re-resolves the SSTable directory
authoritatively EVERY request (`DirSource::resolve`, containment-checked #1430; NOT cached — roborev
1639/#2341), and `WarmTableRegistry::warm_readers` diff/swaps the warm reader set keyed on inode-stable
`GenerationId` (`device, inode, generation`).

Post-#2352 a warm hit is served only when `cached_paths_all_present` — every cached reader's backing
path still `stat`s. In per-query snapshot mode the cached path is the PRIOR (now-cleared) snapshot dir,
so this fails and forces a rebuild. #2383 added rebind-by-inode: on a warm hit with a dead cached path,
if the CURRENT request's resolved dir holds a `Data.db` whose `(device, inode, generation)` + size
match the cached `GenerationId`, `reader.rebind_path(&live.path)` repoints the reader (ArcSwap
`file_path`) and keeps ALL parsed state; a mismatch fails closed to a full rebuild.

So today: parse is elided (good), but the closure is incomplete — every query still pays a
resolve + per-generation `stat` + rebind, the rebind is fragile against a snapshot cleared before the
scan opens its fd (#2352 ENOENT class), and NOTHING reduces the snapshot create/flush churn (#2306).

## The three sub-problems

This change decomposes into: (a) warm-state closure across per-query snapshots, (b) memtable-flush
churn, (c) the interplay lever that serves both — plus (d) observability and (e) failure modes.

---

## §A — Warm-state closure across per-query snapshots (#2356)

### Option 1 — "rebind everywhere" (complete #2383)
Keep inode-stable rebind as THE closure mechanism. #2383 already rebinds the scan path
(`stream_all_partitions_for_compaction → new_scan_cursor → open(&self.file_path)`, now ArcSwap
`file_path`) and the point path uses a held-fd `point_source`.

- **What is still path-bound after #2383:** the rebind FIRES per query and requires (i) an authoritative
  `resolve_dir` (one `read_dir`), (ii) a `GenerationId::resolve` `stat` per cached generation, (iii)
  an identity-matching LIVE entry in the CURRENT resolved dir to rebind onto. If the connector cleared
  the prior snapshot AND has not yet (or ever) created a fresh matching dir for this query, there is no
  live hardlink to rebind onto → fail closed to full rebuild. Rebind therefore only wins when a fresh
  same-inode snapshot dir is present at resolve time — i.e. it is coupled to the connector's snapshot
  cadence.
- **Verdict:** necessary and correct, but INSUFFICIENT alone — it cannot close the churn (create/flush
  fan-out per query) and remains one clear-race away from a rebuild. KEEP it as the correctness
  backstop; do not make it the whole answer.

### Option 2 — durable fd / mmap scans (hold backings; path death irrelevant)
At warm-cache time, hold the `Data.db` fd (and Index/Summary/etc.) open for the reader's lifetime and
serve BOTH scan and point paths off held descriptors (adopt the point path's `pread` `point_source` for
scans — read-path audit F3 territory). Under POSIX unlinked-but-open semantics, a cleared snapshot dir
does not invalidate an open fd, so path death becomes a non-event — no resolve dependency for the DATA
read, no rebind, no #2352 clear race on an in-flight scan.

- **Cost:** an fd (really a small set of fds) held per warm generation for the process lifetime →
  `Σ (generations × components)` descriptors. Bounded by the warm-budget LRU (`with_budget`), but at
  field scale (many generations × many warm tables) this is a real fd-exhaustion surface needing an
  explicit `ulimit`-aware cap and eviction. It is also a larger, parity-sensitive core change to the
  scan path (currently re-opens by path).
- **Verdict:** the most ROBUST closure and the true "path death is irrelevant" answer, but too large +
  fd-risky for the 0.15 lane. **Defer** to bounded future hardening (recommend only if field data
  after Seams A/B still shows rebuild churn). Record the posture; do not implement now.

### Option 3 — connector-side longer-lived / reused snapshots
Covered under §B (it is the shared lever). From §A's perspective: a stable snapshot path across queries
means `resolve_dir` returns the SAME dir → `cached_paths_all_present` PASSES → a pure warm hit with NO
rebind, NO re-parse. The rebind (Option 1) then fires only once per snapshot-window rollover.

### §A recommendation — **Option 3 (keystone) + Option 1 (backstop); Option 2 deferred**
The closure is delivered primarily by making the path STABLE (Option 3), with #2383's inode rebind
(Option 1) as the correctness backstop for the once-per-window rollover and any residual per-query dir.
Option 2 (durable fds) is evaluated and DEFERRED (fd-exhaustion + core-change cost) — recorded as future
hardening. Seam A's NEW work is therefore: (1) pin the end-to-end warm hit through `do_get` in
snapshot mode with scale-free counters, (2) keep the #2352 fail-closed and #2383 rebind invariants
green, (3) expose the closure counters (§D). The authoritative per-request resolve (#2341/#1430) is
UNCHANGED.

---

## §B — Memtable-flush churn (#2306)

Constraint (owner #2305): flush-on-snapshot is BY DESIGN; the Sidecar HTTP API has no `skipFlush`. The
lever is FEWER snapshot CALLS, never skipping the flush.

### Option B1 — snapshot reuse / TTL batching (keyed on `(keyspace, table)`)
Reuse a single snapshot across queries within a bounded freshness window instead of one per `queryId`.
Name it `cqlite-<ks>-<table>-<epoch>` (epoch = the logical freshness window id) rather than
`cqlite-<queryId>`. `SnapshotManager` caches, per `(host, ks, table)`, the current snapshot name +
window; `snapshotFor`/`availableHosts` return the cached name while the window is fresh, create a new
one when it expires or is invalidated, and `cleanup` retires superseded snapshots (a Sidecar TTL stays
the leak backstop). N queries in one window → 1 create fan-out → 1 flush per host, not N.

- **Cost:** reads reflect data up to one window old (documented staleness bound — acceptable for the
  Trino/analytics workload). Slightly more code in `SnapshotManager`; interacts with the #2227
  per-replica-host model (reuse is per host, same as create).
- **Verdict:** RECOMMENDED. Directly reduces flush/create volume by the reuse factor and (via §A) makes
  the path stable. This is the #2306 option-1 "snapshot reuse / TTL batching."

### Option B2 — `skipFlush` via `nodetool`/JMX or a sidecar HTTP contribution
#2306 options 2/3. Moves snapshot creation off the Sidecar HTTP path onto `nodetool`/JMX (adds JMX +
`nodetool` to the flight/connector image) OR contributes `?skipFlush` upstream to
`apache/cassandra-sidecar`.

- **Verdict:** REJECTED for this change. The upstream contribution violates the comment-only rule;
  the JMX/`nodetool` route is an architectural image change that relitigates #2305's HTTP-API posture.
  Park both.

### Option B3 — LIVE-mode routing where isolation is not required
Route workloads that tolerate no point-in-time isolation to `ReadMode.LIVE` (no snapshot, no flush,
inert `SnapshotManager`). Already exists; this change only DOCUMENTS it as the zero-churn option for
callers who accept live reads.

### §B recommendation — **B1 (snapshot reuse/TTL) primary; B3 documented; B2 rejected**
B1 is the lever that reduces flush churn without touching flush semantics. B3 is documented as the
zero-isolation escape hatch. B2 is parked (comment-only + #2305).

### §B addendum — bounded retirement of superseded windows (roborev on the initial impl)
The first implementation retired a superseded window ONLY on explicit `invalidate`/`retireAll`, leaving
supersede-on-roll to the ~6h Sidecar TTL alone. Roborev flagged the resource-retention regression: a hot
table with a 3s window accumulates on the order of `ttl / window` (~7200) live hardlink sets per table
per host until TTL. Decision: retire a superseded window after a bounded **grace period**
(`cqlite.snapshot-retire-grace-ms`, default 10 min) — chosen over per-window in-flight-reader
ref-counting because the connector has no reader-drain hook (the actual reads happen out-of-process on
the Rust flight server), whereas a grace tied to the max query duration is simple, race-free (once the
grace elapses no query that resolved the window can still be reading it), and deterministically testable
on the existing injected clock. Retirement is swept lazily on the next `resolveSnapshot` (no background
thread); steady-state retained superseded dirs bound to ~`grace / window`, well under the TTL backstop
which still covers a crash between supersede and sweep. The counters (`snapshot_creations_total`/
`snapshot_reuse_hits_total`, the #2306 flush proxy) are incremented only after a fan-out fully succeeds,
and a fail-closed partial fan-out rolls back its half-created window rather than caching it.

---

## §C — Interplay: one lever, both problems

**Option 3 / B1 (longer-lived, reused snapshots) is the single keystone that serves BOTH §A and §B.**
A reused snapshot within a freshness window gives:
- **(§A) a stable resolved path** → warm hit with zero rebuild and zero rebind within the window;
- **(§B) fewer creates** → fewer flushes.

### Lifetime / invalidation semantics (no wall-clock in tests)
A reused snapshot for `(keyspace, table)` is valid until the FIRST of:
1. **Window expiry** — the freshness window `W` elapses. `W` is measured on an INJECTABLE logical clock
   (a `Clock`/`Ticker` seam in `SnapshotManager`, `nanos`/epoch counter), never `System.currentTimeMillis`
   in tests. Production default is a configurable `snapshotReuseWindow` (e.g. a few seconds — the
   staleness the analytics workload tolerates); tests advance the injected clock deterministically.
2. **Live-generation-set change** — the table's on-disk SSTable generation set changed (a flush or
   compaction produced/removed generations) since the snapshot was taken. Detection is authoritative:
   the connector observes the resolved generation set (or a cheap generation-set fingerprint the flight
   server can expose) and invalidates on change. NOTE: a reused snapshot is ALWAYS a correct immutable
   point-in-time even across a generation change (Cassandra snapshots are immutable hardlinks) — this
   invalidation is a FRESHNESS lever, not a correctness requirement; it bounds how stale a reused read
   may be when the table is actively changing.
3. **Explicit refresh** — an operator/connector `invalidate(ks, table)` (the `Database::refresh()`
   analog) forces a new snapshot on the next query.

The staleness bound is thus: reads reflect table state no older than `min(W, time-since-last-
generation-change)`, and NEVER an inconsistent mix (each reused snapshot is one atomic point-in-time).
This bound is documented in the connector docs.

---

## §D — Metrics / observability (design, exact names at implementation)

Field rounds must measure the closure round-over-round on the #2399 scoreboard. Counters:
- **`snapshot_creations_total`** (connector, per `(ks, table)`) — snapshot create fan-outs performed.
  Reuse is proven by `creations_total / queries` dropping toward `1 / (queries-per-window)`.
- **`snapshot_reuse_hits_total`** (connector) — queries served by an already-live snapshot.
- **flushes-avoided proxy** — derived from `snapshot_creations_total` (one create ⇒ one flush per host
  under #2305); the connector docs state the derivation. No new server metric needed for #2306's AC.
- **Rust side:** reuse the existing `resolves` (per-request resolve count, service caches),
  `cqlite.sstable.index_parses_total` (#2383; parse-elision proof), `reader_opens` (warm work-probe),
  and add a **`rebind_hits_total`** on the warm registry so a warm-hit-with-rebind is distinguishable
  from a pure warm hit (path unchanged) and from a full rebuild.

All counters are scale-free work-probes (independent of partition count), pinnable by unit/integration
tests without a field fixture, per the #2383 pin style.

---

## §E — Failure modes

1. **Snapshot deleted mid-scan (the #2352 ENOENT class).** With reuse, the window makes this rare, but
   it can still happen at a window rollover or an operator `clearsnapshot`. Invariant preserved: a dead
   cached path with NO identity-matching live entry FAILS CLOSED to a rebuild (never ENOENT mid-merge,
   never a stale serve). If Option 2 (durable fds) were later adopted, an in-flight scan would complete
   over the held backing; under the recommended package it fails closed and the query re-runs cold — no
   correctness loss.
2. **fd exhaustion.** Only relevant if Option 2 is later adopted; the recommended package does NOT hold
   extra fds beyond today's point-`source`, so no new fd surface. Recorded so the deferred option
   carries an explicit `ulimit`-aware cap requirement when picked up.
3. **Staleness vs the per-request probe (#2341).** The Rust resolve stays authoritative per request BY
   DESIGN; reuse changes only the snapshot NAME the connector passes, so the server-side containment +
   liveness guarantees are unchanged. A reused snapshot that was retired server-side (TTL fired) resolves
   to a dead dir → the §E-1 fail-closed rebuild path, never a stale serve.
4. **Reuse across a schema/DDL change.** `WarmTableRegistry` already keys on a `ddl_hash`; a DDL change
   invalidates the warm set independently. Snapshot reuse additionally invalidates on the
   generation-set change a DDL-driven flush produces, so no stale-schema serve.

## Composition with #2412 (lazy Summary-guided index)
#2412 makes BIG open O(summary) and warm resident memory ≈ summary. This change is orthogonal: it never
redefines index open/walk, only READS `index_parses_total` and the warm-registry hit path. Post-#2412,
a cold rebuild is cheaper (so the §A closure is less costly to miss), but the snapshot create/flush
churn (§B) and the per-query resolve+stat are unchanged — this change still delivers the churn reduction
and the stable-path warm hit. The two land independently; whichever merges second rebases onto the
other with no requirement overlap (verified: #2412's spec touches `cqlite-core` reader/index; this
change touches `cqlite-flight` warm registry + `trino-connector` SnapshotManager).

## Recommended package (single, for Seam 1 approval)
- **§A:** Option 3 keystone + Option 1 (#2383 rebind) backstop; Option 2 durable-fd DEFERRED.
- **§B:** Option B1 (snapshot reuse/TTL) primary; B3 (LIVE-mode) documented; B2 (skipFlush) rejected.
- **§C:** the reused snapshot is the shared lever; lifetime = `min(window W, generation-set change,
  explicit refresh)`, injectable logical clock, documented staleness bound, no wall-clock in tests.
- **§D:** `snapshot_creations_total` + `snapshot_reuse_hits_total` (connector), `rebind_hits_total`
  (Rust) + existing resolve/parse/opens counters.
- **§E:** fail-closed on mid-scan deletion preserved; no new fd surface; staleness bounded and
  documented.
