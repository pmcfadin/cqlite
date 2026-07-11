# Tasks — cqlite-flight warm-handle service (#2310)

One issue ↔ branch `issue-2310-flight-warm-handles` ↔ this change ↔ one PR. Each stage names the
public surface it exercises and carries a red-then-green test (fails on `main`). Anchors are
`main`-relative and WILL drift — re-grep before editing. Follow the implement loop: `--lite`
(summary-file redirect) each fix round → rust-reviewer + roborev on the lite-green diff
(review-first) → open PR → hand the endgame to `flow-closer` (ONE full gate → C intent audit →
final roborev → merge-on-green → finalize). Point `CQLITE_DATASETS_ROOT` at the main repo's
`test-data/datasets`. The WS numbering maps to the epic's WS1–WS5.

## WS1 — generation-identity + warm reader-set abstraction (adopt #1749 contract)
- [ ] 1.1 Introduce a flight-owned warm-handle registry keyed on generation identity (Decision 1):
  `(table, inode-stable generation set)` with device+inode identity cross-checked against
  `generation_of` (`cqlite-flight/src/producer.rs:237`). Surface: a `WarmTableRegistry` (or
  equivalent) that hands `MergeProducer` a pre-resolved, pre-parsed reader/path set instead of
  `DirSource::resolve` + cold reader-open. (flight-warm-handles)
- [ ] 1.2 Model the warm set's diff/swap on `SSTableManager::refresh_tables`
  (`cqlite-core/src/storage/sstable/refresh.rs`, #1749): open **added** generations, drop
  **removed**, keep **unchanged** parsed state; atomic swap under a write guard; in-flight requests
  hold `Arc` clones. Adapt the seam (Open Question 1) rather than route through `Database`. (flight-warm-handles)
- [ ] 1.3 Red-then-green test: two `do_get`s for the same table reaching two different snapshot
  hardlink dirs over the same inodes resolve to ONE warm entry; the second is a warm hit with zero
  reader-open/parse. Fails on `main` (no warm state). Proves *Requirement: generation-identity
  key*. (flight-warm-handles)

## WS2 — refresh-trigger (the Seam-1 decision: per-request probe + manifest fast path)
- [ ] 2.1 Per-request generation-set staleness probe (Decision 2a): enumerate the current
  generation set, diff against the cached set; unchanged → warm hit (skip all reader-open/parse),
  changed → rebuild the delta. Authoritative listing, no mtime/heuristic inference (#28). Surface:
  the warm-handle lookup path in `do_get_setup` (`cqlite-flight/src/service.rs:479`). (flight-warm-handles)
- [ ] 2.2 Snapshot `manifest.json` fast path (Decision 2b): byte-identical manifest → warm hit
  without a `read_dir`; absent/unparsable manifest → fall back to the authoritative probe. (flight-warm-handles)
- [ ] 2.3 Fail-closed (Decision 2, mirrors #1749): probe error → treat as "changed" (full
  re-resolve); rebuild error (added generation fails to open, #1626) → previously warm set stays
  fully intact, typed error surfaced. (flight-warm-handles)
- [ ] 2.4 Red-then-green tests: unchanged set → warm hit, zero parse (work-done probe); added
  generation → visible next request, zero staleness window; matching manifest → no `read_dir`;
  absent manifest → authoritative-probe fallback; corrupt added `Statistics.db` → old set served
  intact. Proves *Requirements: per-request staleness probe*, *manifest fast path*, *fail-closed
  refresh*. (flight-warm-handles)

## WS3 — snapshot-mode integration (hardlink/inode dedup validation)
- [ ] 3.1 Wire the warm handle into snapshot resolution (`DirSource::resolve` snapshot branch,
  `cqlite-flight/src/producer.rs:146`): per-query snapshot dirs stay (isolation + #2305
  flush-on-snapshot unchanged) but resolve to the shared inode-keyed warm entry. (flight-warm-handles)
- [ ] 3.2 Validate hardlink/inode dedup on a real per-query-snapshot fixture: two snapshot dirs over
  the same inodes share one warm entry and return byte-identical batches; assert no snapshot-semantic
  change (point-in-time result unchanged vs `main`). Proves the cross-snapshot warm-hit scenario. (flight-warm-handles)

## WS4 — memory budget + eviction + metrics
- [ ] 4.1 LRU eviction by (table, generation) with explicit byte accounting inside the <128MB
  discipline (Decision 4); a removed-on-disk generation evicts immediately. Surface: the registry's
  bounded-capacity policy. (flight-warm-handles)
- [ ] 4.2 Bounded observability counters — warm hit / miss / evict / refresh-outcome
  (unchanged / rebuilt-delta / fail-closed-retained) — riding the existing observability contract,
  no new knob/env/ticket field. (flight-warm-handles)
- [ ] 4.3 Red-then-green tests: exceeding the budget evicts LRU (footprint stays bounded, evicted
  entry re-parses on next request); metrics distinguish miss/hit/rebuild. Proves *Requirements:
  memory budget + eviction*, *metrics counters*. (flight-warm-handles)

## WS5 — cancellation + bench evidence
- [ ] 5.1 Cancellation through the warm path (Decision 5, #2264/#1473): pre-cancelled request does
  zero probe/rebuild work and surfaces the `Cancelled` variant; disconnect mid-rebuild leaves the
  warm set intact. Red-then-green test. Proves *Requirement: cancellation through cached paths*. (flight-warm-handles)
- [ ] 5.2 Bench evidence on the #2289 harness (point read, LIMIT, full scan; ≥100k-partition table)
  + #1494 bench suite: second identical query on an unchanged generation shows ~0 parse cost vs the
  cold run, warm-hit counter increments, rows byte-identical. Proves *Requirement: bench evidence*. (flight-warm-handles)
- [ ] 5.3 e2e wiring evidence: a real `FlightService::do_get` over the tonic transport, run twice for
  the same table/generation, drives the warm path end-to-end (metrics show miss then hit) and returns
  identical rows — the named public surface exercising the warm handle. (flight-warm-handles)

## WS6 — endgame (flow-closer)
- [ ] 6.1 `--lite` green on the full diff (summary-file redirect); rust-reviewer + roborev on the
  lite-green diff (review-first); fix rounds re-run `--lite` + diff-scoped parity/integration targets.
- [ ] 6.2 Open PR; hand to `flow-closer`: ONE full `scripts/agent-gate.sh` (run of record) →
  spec-auditor **C** intent audit anchored to `specs/flight-warm-handles/spec.md` → final roborev →
  merge-on-green (`gh pr merge --squash --delete-branch`) → `flow-finalize` (archive change, close
  the WS child / update epic #2310, telemetry stamp).
