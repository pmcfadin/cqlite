# Tasks — Global bounded key→partition-offset cache (#2059)

One branch `issue-2059-bounded-key-cache`, staged commits. Each task names the surface it exercises and
carries a red-then-green test (scale-free work/memory probes in the #2383/#2385 pin style). Anchors are
`main`-relative and will drift — re-grep before editing. **Seam 1** (owner approval of proposal + design,
incl. §A global scope/key, §B two-caps-one-envelope budget model, §D sequence-after-#2412) precedes all
implementation. **Sequenced AFTER #2412** (design §D): the point-lookup wiring (Stage 2) targets #2412's
Summary-guided interval-parse fast path; if #2412 is not yet merged, this change blocks on it for the
work-probe + wiring-evidence scenarios.

## Stage 0 — probes first (must fail before wiring)
- [ ] 0.1 Aggregate-memory probe: total resident bytes stay `<= B` across `N` distinct generations, far
  more than `B` holds — independent of reader count. Surface: `GlobalKeyOffsetCache` resident-bytes
  accessor. (global-key-offset-cache)
- [ ] 0.2 Work-probe (post-#2412): a repeated present-key point read touches `1` then `0`
  `cqlite.sstable.index_interval_parses_total` (miss populates, hit skips the interval parse). Surface:
  `big_get_with_resolution` + read-work/interval-parse counters. (global-key-offset-cache)
- [ ] 0.3 Invalidation-correctness probe: a removed generation's cached entry never serves rows
  (query-semantics oracle at a pinned `now`). Surface: cache invalidation hook + flight warm evict.
  (global-key-offset-cache)

## Stage 1 — the global cache type
- [ ] 1.1 Add `GlobalKeyOffsetCache` (`cqlite-core/src/storage/cache/`): single instance, sharded
  `Mutex<Shard>` (power-of-two, high shard count per §F), byte-bounded LRU, keyed on
  `(generation identity, raw partition key) → PartitionLoc`, poison-tolerant, `disabled()` no-op. Unit
  pins: eviction order, byte-bound, no-alias-across-generations, poison recovery, concurrency soundness.
  (global-key-offset-cache)
- [ ] 1.2 Retire the per-reader `KeyOffsetCache` construction (`build_key_offset_cache`,
  `reader/mod.rs`, `reader/types.rs`): readers hold a shared `Arc<GlobalKeyOffsetCache>` handle instead of
  a private cache. (global-key-offset-cache)

## Stage 2 — point-lookup wiring (§B, on the #2412 interval-parse path)
- [ ] 2.1 Consult/populate the global cache with the reader's authoritative generation identity at the
  #2412 Summary-guided point-lookup site (`lookup_partition_with_index` /
  `data_access/big_point.rs::big_get_with_resolution`): hit → skip the interval parse; miss → one interval
  parse → populate. Prove 0.2 work-probe green. (global-key-offset-cache)

## Stage 3 — invalidation (§C)
- [ ] 3.1 Invalidate all entries for a generation identity on generation removal / compaction /
  `WarmTableRegistry` evict (#2310 diff/swap, #2343 removed-on-disk); distinct `invalidations` counter.
  Fail-closed on identity mismatch (`get` supplies current identity). Preserve #2383 rebind-stability
  (unchanged identity → entries survive). Prove 0.3 probe green + the rebind-stability + identity-mismatch
  scenarios. (global-key-offset-cache)

## Stage 4 — metrics (§G)
- [ ] 4.1 Global snapshot (hits/misses/evictions/invalidations/resident-bytes/capacity-bytes),
  `cqlite.`-namespaced, catalog-registered, reported via `Database::stats().memory_stats` as one
  consolidated envelope; disabled cache reports honest zeros. Prove: counter-activity + catalog-namespacing
  scenario. (global-key-offset-cache)

## Stage 5 — wiring evidence (flight do_get, cold + warm)
- [ ] 5.1 Flight `do_get` cold + warm e2e: cold populates (miss → interval parse), warm hits (0 additional
  interval parses over unchanged generations), rows match the query-semantics oracle. Surface: flight
  `do_get` / warm producer. (global-key-offset-cache)
- [ ] 5.2 Doctrine: one-line note on the global key cache to the source-map / format-debugging
  `agents-developing/` page + any dev-cookbook pointer (keep-doctrine-current rule).

## Stage 6 — endgame (definition of done)
- [ ] 6.1 `--lite` (summary-file redirect) each fix round; blast-radius targets + diff-relevant
  parity/integration targets.
- [ ] 6.2 rust-reviewer + roborev on the lite-green diff (review-first); fix blockers → re-lite +
  re-review; batch nits into ONE follow-up issue at merge.
- [ ] 6.3 Open PR. `flow-closer`: FULL `scripts/agent-gate.sh` ONCE → PASS (record SUMMARY); spec-auditor
  **C** anchored to `openspec/changes/bounded-key-cache/specs/**` → PASS; final roborev clean;
  merge-on-green (`gh pr merge --squash --delete-branch`); `flow-finalize` (close #2059, telemetry).
- [ ] 6.4 `openspec archive bounded-key-cache`.
