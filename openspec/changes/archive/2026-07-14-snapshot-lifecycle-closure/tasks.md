# Tasks — Snapshot lifecycle closure

Sequenced on branch `issue-2356-snapshot-lifecycle-closure` (staged commits). Each stage names the
public surface it exercises and carries a red-then-green test. Anchors are `main`-relative and will
drift; the implementer re-greps before editing. `CQLITE_DATASETS_ROOT` points at the MAIN checkout's
`test-data/datasets` (a worktree lacks the gitignored `Data.db` binaries — 0-row false passes).

## Stage 0 — red probes first (must fail before the fix)
- [ ] 0.1 Add a `rebind_hits_total` field + accessor to the warm metrics
  (`cqlite-flight/src/warm/metrics.rs`), and a flight-level probe test asserting the FOUR-request
  sequence (cold / stable-repeat / fresh-same-inode / changed-inode) counter deltas — fails on `main`
  (no `rebind_hits_total` yet). (flight-warm-snapshot-closure)
- [ ] 0.2 Add a `SnapshotManagerTest` (Java, `trino-connector/src/test/...`) with an injected logical
  clock asserting "N queries in one window ⇒ 1 create" — fails on `main` (one create per `queryId`).
  (flight-snapshot-reuse)

## Stage 1 — Seam A: warm-state closure (Rust `cqlite-flight`)
- [ ] 1.1 Wire `rebind_hits_total`: increment on each `reader.rebind_path(&live.path)` in
  `warm/registry.rs` (the #2383 rebind pass); expose via `WarmMetrics::snapshot` and the flight stats
  surface (`service.rs` metrics). (flight-warm-snapshot-closure)
- [ ] 1.2 End-to-end `do_get` pin (snapshot mode): repeat query over a STABLE resolved snapshot path
  asserts `index_parses_total` and `reader_opens` deltas of 0 (pure warm hit); a fresh same-inode dir
  asserts `rebind_hits_total` delta > 0 with `index_parses_total` delta 0. Exercise through the flight
  `do_get` path, not the registry helper alone. (flight-warm-snapshot-closure)
- [ ] 1.3 Keep the #2352 fail-closed + #2383 changed-inode-rebuild invariants green (dead path, changed
  inodes ⇒ rebuild; dead path, no live match ⇒ clean rebuild, never ENOENT/stale). Extend
  `warm/spin_tests_2383.rs` / `warm/registry_tests.rs`. (flight-warm-snapshot-closure)
- [ ] 1.4 Confirm the per-request `resolves` counter still increments exactly once per request
  (authoritative resolve UNCHANGED, #2341/#1430). (flight-warm-snapshot-closure)

## Stage 2 — Seam B: snapshot reuse / TTL (Java `trino-connector/SnapshotManager`)
- [ ] 2.1 Introduce an injectable `Clock`/`Ticker` seam + a `snapshotReuseWindow` config
  (`CqliteFlightConfig`); default a small window; tests inject a manual clock (no
  `System.currentTimeMillis`). (flight-snapshot-reuse)
- [ ] 2.2 Reuse cache keyed on `(host, keyspace, table)` → (snapshot name, window epoch, generation-set
  fingerprint). `snapshotFor`/`availableHosts` return the cached name while fresh; create on expiry /
  generation-set change / explicit `invalidate(ks, table)`; name `cqlite-<ks>-<table>-<epoch>`.
  Retire superseded snapshots via `cleanup` (Sidecar TTL stays the leak backstop). (flight-snapshot-reuse)
- [ ] 2.3 Add `snapshot_creations_total` + `snapshot_reuse_hits_total` connector counters; assert the
  window/expiry/refresh/generation-change scenarios. (flight-snapshot-reuse)
- [ ] 2.4 Preserve `ReadMode.LIVE` inertness (no reuse cache, no creates); assert. (flight-snapshot-reuse)
- [ ] 2.5 Query-semantics parity: a read from a reused snapshot matches the oracle at a pinned `now`.
  (flight-snapshot-reuse)

## Stage 3 — docs + observability
- [ ] 3.1 Connector docs (`docs/flight-trino/`): snapshot reuse, the `min(window, generation-change)`
  staleness bound, the `creations ⇒ flushes` derivation for the #2306 cost report, and the LIVE-mode
  zero-churn escape hatch. One-line note on the flight/source-map page per keep-doctrine-current.
- [ ] 3.2 Record the DEFERRED durable-fd (§A Option 2) posture + the fd-cap requirement in the connector
  docs / a follow-up issue stub, so field data can reopen it.

## Stage 4 — endgame (standard loop)
- [ ] 4.1 `--lite` each fix round (summary-file redirect); iterate to PASS.
- [ ] 4.2 Review-first: `rust-reviewer` + roborev on the lite-green diff; fix blockers, re-`--lite`.
- [ ] 4.3 Open PR; `flow-closer` runs the FULL gate ONCE → spec-auditor **C** (every requirement
  `satisfied` with a public-surface test) → final roborev → merge-on-green → `openspec archive`.
- [ ] 4.4 Telemetry stamp (`flow-finalize`); batch nits into ONE linked follow-up at merge.
