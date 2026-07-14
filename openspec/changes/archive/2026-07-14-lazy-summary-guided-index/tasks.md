# Tasks — Lazy Summary-guided BIG partition index (#2412)

One branch `issue-2412-lazy-summary-guided-index`, staged commits. Each task names the surface it
exercises and carries a red-then-green test (scale-free work/memory probes in the #2383/#2385 pin style).
Anchors are `main`-relative and will drift; re-grep before editing. Seam 1 (owner approval of proposal +
design, incl. the §A absent-Summary posture and the §E #2413 interplay choice) precedes all
implementation.

## Stage 0 — probes first (must fail on main)
- [ ] 0.1 Add a scale-free work-probe: cold BIG open touches 0 `Index.db` entries + 0 full parses
  (fails today — open full-parses). Surface: `SSTableReader::open` + read-work counters.
  (lazy-big-partition-index)
- [ ] 0.2 Add the interval-parse counter to the observability catalog
  (`cqlite.sstable.index_interval_parses_total`) alongside `cqlite.sstable.index_parses_total`; catalog
  registration/namespacing test. (lazy-big-partition-index)
- [ ] 0.3 Add a resident-memory probe/dhat budget for a warm-held generation (fails today — full map
  pinned). Surface: `WarmTableRegistry`. (lazy-big-partition-index)

## Stage 1 — Summary find-by-key primitive
- [ ] 1.1 Add `SummaryReader::find_by_key` (binary search over sampled keys → `Index.db` start position)
  + a lazy `Index.db` interval accessor (seek + parse ≤ `min_index_interval` entries). Unit-pinned
  against a real BIG `Summary.db`/`Index.db`. Surface: `SummaryReader` (`summary_reader.rs`).
  (lazy-big-partition-index)

## Stage 2 — lazy BIG open (§A)
- [ ] 2.1 Stop materializing `IndexData.partition_entries` at open; `IndexReader` /
  `load_index_reader` (`reader/component_loading.rs`, `index_reader/mod.rs`) hold the summary + a lazy
  `Index.db` accessor. Open loads `Summary.db` only. (lazy-big-partition-index)
- [ ] 2.2 §A1 absent/corrupt-`Summary.db` FellBack: one counted full `Index.db` parse (increments
  `index_parses_total`, surfaces a FellBack reason); preserve `is_fully_parsed()`/Signal-A completeness.
  Prove: 0.1 probe green + the counter-semantics scenario. (lazy-big-partition-index)

## Stage 3 — point lookup (§B)
- [x] 3.1 `big_get_with_resolution` (`data_access/big_point.rs`): after the C5 short-circuit, binary-
  search the summary → read ONE `Index.db` interval → resolve. Within-range absent = authoritative (no
  whole-file `scan_for_key`). Interval-parse counter increments once/lookup. (lazy-big-partition-index)
  Routed via `lookup_partition_with_index` → new `reader/summary_point.rs`
  (`should_use_summary_interval` + `lookup_partition_via_summary_interval`); the authoritative-absence
  claim is gated on an END-BOUNDED interval (`covering_interval_is_end_bounded`) so a tail-truncated
  `Index.db` (the #1572 class, whose dropped entries live in the last read-to-EOF interval) keeps the
  scan fallback.
- [x] 3.2 Prove: present-key + within-range-absent + interval-boundary point reads with `Index.db`
  entries touched ≤ one interval — `tests/issue_2412_point_interval.rs` (public `get()` surface, work
  probes: interval-parse == 1, full-parse == 0, no `scan_for_key` on the authoritative-absent path).
  Byte-identical goldens covered by the query-semantics + sstabledump parity oracles.
  (lazy-big-partition-index)

## Stage 4 — scans (§C) + #2413 posture (per Seam-1 choice)
- [ ] 4.1 Summary-guided forward iteration feeding the #2361 streaming walk (`full_index_stream`);
  preserve the `(token, key)` order guard, FellBack gating, cancel-aware teardown, and Signal-A terminus.
  `iterate_all_partitions_via_full_index` no longer consumes a resident `Vec`. (lazy-big-partition-index)
- [ ] 4.2 Option A (recommended): range-scoped split begins at the summary sample covering the range
  start, stops at range end; flip the #2413 pin (`walked <= 4` single-partition warm scan). Compaction
  consumers keep full-ring (no range). FellBack (no summary) posture explicitly adjudicated + counted.
  Prove: physical-dump + query-semantics oracles + compaction byte-parity. (lazy-big-partition-index)

## Stage 5 — warm-registry memory (§D)
- [ ] 5.1 `WarmTableRegistry` pins summary-only; update `warm/budget.rs` per-generation cost estimate +
  `warm/metrics.rs` gauges; keep #2383 inode identity/rebind + #2310 diff/swap eviction. Prove: 0.3
  resident-memory probe green (summary-only footprint). (lazy-big-partition-index)

## Stage 6 — wiring evidence (§G, AC7)
- [ ] 6.1 Flight `do_get` cold + warm e2e: cold resolves rows (query-semantics oracle) with 0 full
  parses; warm repeat = 0 reader-opens + 0 full parses over unchanged generations. Surface: flight
  `do_get` / `producer_warm`. (lazy-big-partition-index)
- [ ] 6.2 Doctrine: one-line note on the lazy BIG open to the format-debugging / source-map
  `agents-developing/` page + any dev-cookbook pointer (keep-doctrine-current rule).

## Stage 7 — endgame (definition of done)
- [ ] 7.1 `--lite` (summary-file redirect) each fix round; blast-radius targets + diff-relevant
  parity/integration targets.
- [ ] 7.2 rust-reviewer + roborev on the lite-green diff (review-first); fix blockers → re-lite +
  re-review; batch nits into ONE follow-up issue at merge.
- [ ] 7.3 Open PR. `flow-closer`: FULL `scripts/agent-gate.sh` ONCE → PASS (record SUMMARY); spec-auditor
  **C** anchored to `openspec/changes/lazy-summary-guided-index/specs/**` → PASS; final roborev clean;
  merge-on-green (`gh pr merge --squash --delete-branch`); `flow-finalize` (close #2412, telemetry).
- [ ] 7.4 If Seam 1 chose Option A, close #2413 into this change; if Option B, keep #2413 open with its
  pin preserved. `openspec archive lazy-summary-guided-index`.
