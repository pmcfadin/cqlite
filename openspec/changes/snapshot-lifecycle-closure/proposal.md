# Snapshot lifecycle closure (#2356 primary, #2306 secondary)

## Milestone
0.15 — the cqlite-trino latency/throughput/operations theme (epic #2403, **Lane 3**). Serves the
Trino latency/throughput/operations lanes; acceptance measured round-over-round on the #2399 field
scoreboard.
**Design-driven — OpenSpec + Seam 1 required before any implementation.** Both anchored issues route
design-driven (their bodies say so). Oracle-driven correctness (rows returned) stays pinned by the
existing physical-dump + query-semantics parity nets; this change redesigns the *per-query snapshot
lifecycle* — how a snapshot is created, reused, resolved, and how warm parsed state survives it —
which is a structural/operational decision, hence OpenSpec.

## Issue anchors
- **#2356 (primary, P2)** — flight warm handles are a rebuild-per-query no-op in per-query-snapshot
  mode. Post-#2352 a warm hit is served only when every cached reader's backing path still resolves;
  the connector's default per-query snapshot mode clears the prior snapshot dir, so the cached path is
  always dead → every query re-opens + re-parses. #2383 delivered rebind-by-inode (parse elided when a
  fresh matching snapshot dir presents), but the closure is incomplete: it still depends on a
  per-query resolve+stat+rebind against a dir that may already be gone, and the underlying churn
  remains.
- **#2306 (secondary, P3)** — the per-query snapshot flushes the queried table's memtable, producing
  tiny-SSTable spam + compaction churn on the production cluster under query-heavy workloads. Owner
  decision (#2305, 2026-07-09): flush-on-snapshot is BY DESIGN for the Sidecar HTTP API (no
  `skipFlush`; JMX-only). #2306 tracks the remaining OPERATIONAL cost, not the flush semantics.

These two issues share ONE design space — the per-query-snapshot lifecycle — so ONE change covers
both. The keystone lever (a longer-lived / reused snapshot) serves both: a stable snapshot path lets
warm state stay warm (#2356) AND fewer snapshot creations means fewer flushes (#2306).

## Why
Field baselines on #2367 (rounds 9/10) and the #2356/#2306/#2398 issue bodies:

1. **Warm benefit does not reach the field's default mode.** #2310's warm parse-elision (~830ms
   parse elided, ~1,350× on repeated point reads flight-direct) applies to LIVE mode and stable
   snapshot paths only. In per-query snapshot mode each query gets a fresh `cqlite-<queryId>` snapshot
   dir and the prior dir is cleared, so the cached path is always dead. #2383 rebinds parsed state
   onto the new matching dir, but only when the current request has already resolved a fresh dir whose
   `Data.db` inodes match — a per-query resolve+stat+rebind that is fragile against the #2352 clear
   race and does nothing to reduce the snapshot churn itself.
2. **Per-query snapshot creation is a triple sidecar fan-out.** `SnapshotManager` (`trino-connector/`)
   creates `cqlite-<queryId>` on EVERY replica host (issue #2227: a Sidecar snapshot PUT is
   instance-local), so a scan pays 3× create round-trips per query, then a best-effort clear at query
   end. #2398 attributes ~4-5s of fixed warm-scan setup to resolve/merge_setup; the snapshot
   create/clear fan-out is a lifecycle-owned slice of that.
3. **Every query flushes the queried table's memtable.** Flush-on-snapshot (accepted by #2305) means
   one flush per query per replica host — tiny SSTables + compaction churn scaling with query volume,
   not data volume (#2306).

Individually tolerable; together they defeat the entire warm-handle investment in the field's default
mode and impose a query-rate-proportional operational tax on the cluster.

## What Changes
- **Seam A — warm-state closure across the snapshot lifecycle (Rust `cqlite-flight`, #2356):** make
  the warm hit VERIFIABLY survive the per-query snapshot lifecycle end-to-end through `do_get`, keeping
  #2383's inode-stable rebind as the correctness backstop and pinning it with scale-free work-probe
  counters (index parses, reader opens, rebind hits). No change to the authoritative per-request
  directory resolve (#2341/#1430 — kept BY DESIGN).
- **Seam B — snapshot reuse / TTL (Java `trino-connector/SnapshotManager`, #2356 + #2306):** the
  keystone. Instead of one snapshot per `queryId`, reuse a per-`(keyspace, table)` snapshot within a
  bounded freshness window, invalidated by window expiry or an observed live-generation-set change.
  Fewer snapshot creations → a stable snapshot path (Seam A stays warm, zero rebind churn within the
  window) AND fewer flushes (#2306). The lever is FEWER snapshot CALLS, never skipping the flush
  (#2305 is not relitigated).
- **Metrics:** snapshot creations per N queries, flushes avoided (creation-rate proxy), rebind hits,
  parse-elision hits — so field rounds can measure the closure round-over-round on #2399.

## Non-goals
- **No change to snapshot correctness or isolation semantics.** A reused snapshot is still a valid,
  immutable Cassandra point-in-time; the only observable change is a bounded, documented staleness
  window for analytics reads. Row-level parity (physical-dump + query-semantics oracles) is inviolable.
- **#2305's flush-on-snapshot verdict is NOT relitigated.** No `skipFlush`, no JMX/`nodetool` in the
  flight image, no per-snapshot flush-skipping. The only lever on flush volume is fewer snapshot calls.
- **No upstream `apache/cassandra-sidecar` or `easy-db-lab` changes** (comment-only rule). The
  `?skipFlush` HTTP-endpoint contribution (#2306 option 3) is explicitly parked, not pursued here.
  `SnapshotManager` lives in OUR `trino-connector/`, so its reuse/TTL logic is an in-tree change.
- **No overlap with #2412 (lazy Summary-guided BIG index).** #2412 owns how the partition index is
  opened and walked (open O(summary), resident memory ≈ summary). This change owns the snapshot
  *lifecycle* around the reader — orthogonal. With #2412 landed, a cold rebuild is cheaper, but the
  resolve+stat per query and the snapshot create/flush churn remain, so this change stays valuable and
  independent. Where the two touch (warm-registry memory, `index_parses_total` semantics), this change
  only READS the counters #2412 defines; it does not redefine index open/walk.
- **No change to the public `Database`/`QueryRow`/flight `do_get` result contract** — same rows, same
  bytes; only the snapshot lifecycle and warm-hit path change.
- **No pre-`na` format support** introduced or revisited (version floor unchanged).

## The #2398 / #2413 boundary (design MUST stay clear of)
#2398 (warm-scan fixed ~4-5s setup) and its fix #2413 (token-range pushdown into the per-SSTable walk)
own the *merge_setup / token-filter* slice of setup cost. THIS change owns the *snapshot lifecycle*
slice (create/clear fan-out, resolve+stat, flush churn). The design (§B, §interplay) states the split
explicitly so the two lanes do not double-count or collide.

## Doctrine impact
- **No-heuristics (#28) preserved:** inode identity (`device, inode, generation` + size) remains the
  ONLY key for rebind; snapshot reuse is keyed on explicit `(keyspace, table)` + a logical freshness
  epoch, never inferred from content. Staleness/invalidation is authoritative (window + generation-set
  change), never guessed.
- **No wall-clock in tests:** the reuse freshness window and invalidation are driven by an injectable
  logical clock / epoch so parity and unit tests pin behavior deterministically (per the #1742
  query-semantics-oracle pinned-`now` discipline).
- CLAUDE.md / website `agents-developing/`: no doctrine text change; add a one-line note to the
  flight/trino connector docs describing snapshot reuse + the staleness bound once implemented
  (in-change, per the keep-doctrine-current rule).

## Definition of done
`scripts/agent-gate.sh` full PASS (SUMMARY recorded) + spec-auditor **C** PASS (every requirement
`satisfied` with a public-surface test) + roborev clean; `RUSTFLAGS="-D warnings"` clean; no
`unwrap()`/`expect()` in library code; physical-dump + query-semantics parity + flight `do_get`
warm+snapshot-mode e2e green; the connector test suite (`trino-connector`) green. Then
`openspec archive`.
