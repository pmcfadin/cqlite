# Design: Weight-balanced splits via drain-hardened sub-splitting

## Context

Post-revert (main), `CqliteFlightSplitManager.buildSplits` emits one split per read-replica range; the
primary owner is `rotate(sortedOwners, floorMod(range.startToken, n))[0]` (#2397/#2409). No `getSplitWeight()`
override exists (both splits report SPI default 1.0). No per-range size feed exists (Sidecar `ReplicaInfo`
= `{start,end,replicas}`; Flight `table_stats` is whole-table). The scan page source's `close()` releases
the Flight stream via `handle.close()` but shows no explicit `FlightStream.cancel()`; this is only proven
safe at split-count == range-count (one `DoGet`/range). The archived change
`2026-07-20-weight-balanced-splits` (PR #2779) added K-way slicing + SplitWeight and was reverted (#2791)
for the #2782 `LIMIT` hang — the slice math was sound; the hang was the un-drained stream × 4 splits.

## Chosen approach: resurrect the archived slice design, add drain fix + LIMIT/point-read K=1 + E2E gate

Statistical balance is the right tool when weights are unknown: dealing K smaller slices per range
round-robin across owners evens load regardless of where weight concentrates, and it is the only approach
that can break a single heavy range across pods (the throughput ceiling). Ordering — the drain fix lands
and is provable FIRST, then slicing is enabled:

1. **Drain fix (prerequisite).** In the scan page source `close()` (and the failover-stream wrapper), on
   early close explicitly **cancel** the active Flight `DoGet` stream (Arrow `FlightStream.cancel(reason,
   cause)` / the client handle's cancel), idempotent and non-blocking, propagated to the currently-active
   underlying stream — not merely `handle.close()`. Unit-test the cancel path with a fake stream; this
   fixes #2782 at root even for K=1.
2. **K=1 exemption for LIMIT-pushed + bound-key point reads.** In `getSplits`, choose effective K: K=1 when
   `handle.limit().isPresent()` OR the read is a fully-bound point read; else the configured K. Keeps the
   hang shape out of the multi-stream path independent of the drain fix (defense in depth), and avoids
   sub-splitting work that early-terminates anyway.
3. **Slicing seam + weight-balanced rotation** (from the archived design): a pure `TokenRangeSlicer`
   (equal-span, overflow-safe unsigned 64-bit, half-open, wraparound-correct, exact cover, no empty slice),
   applied at the single split-manager seam before scan-split construction, the snapshot chooser, and
   pruning. Slice primaries chosen by deterministic per-parent-range rotation; per-owner assigned span
   within ≤1.25× of mean. Aggregate path exempt (K=1), its snapshot chooser at K=1.
4. **`getSplitWeight()` ∝ token span**, mean-span → `standard()`, clamped to `fromProportion` bounds;
   aggregate = clamped sum of member proportions, additionally clamped to a documented aggregate maximum.
5. **E2E LIMIT-hang regression** in `docker/e2e-test.sh` (`LIMIT 2` + partial-predicate `LIMIT`) at the
   default K, wired into `flight-trino-e2e`; a red lane is a hard merge block (no `--auto` over red).

Config `cqlite.sub-splits-per-range` (default 4, min 1, max 64); K=1 is exact pre-change behavior.

### What it beat

- **Token-span LPT, count-invariant (one split/range).** Lower risk (cannot reproduce #2782) but weaker
  throughput: moves whole ranges by a proxy (span) that may not track CPU, and CANNOT split a single heavy
  range — the dominant skew source. Uncertain to lift the 39-qps ceiling. Rejected as the primary; its
  span-weighting idea survives as the `getSplitWeight()` mechanism here.
- **Per-range size feed + true LPT.** Best balance quality but needs a new Flight/Sidecar per-range
  scoping across the Rust core + server, right before a release. Deferred to a follow-up; this change needs
  no feed. If the field CPU AC misses, this is the next lever.
- **Learned/observed cross-query weights.** Stateful, cold-start-blind, conflicts with the deterministic
  assignment AC. Rejected.

## Risks / edge cases

- **The #2782 hazard class is deliberately re-opened**, then fixed at root (drain) AND avoided for the
  trigger shape (LIMIT/point-read K=1) AND guarded by the E2E that caught it. All three must hold; the
  E2E gate is the backstop against a drain regression.
- **`--auto` discipline:** the prior P0 landed because a red E2E was misread and auto-merge armed anyway.
  The closer MUST treat a red `flight-trino-e2e` as a hard block for THIS issue.
- **Slice boundary token assignment** must match server `tokenInRange` so no partition is double-counted
  or dropped at a slice seam (covered by the wraparound scenario).
- **SplitWeight clamping:** near-zero and huge spans must stay within Trino `fromProportion` bounds (no
  zero weight, no exception).
- **Snapshot cover at slice granularity:** `distinctReplicaHosts` must equal the set of slice primaries so
  snapshot-mode reads never hit an unpinned host (#2227 fail-closed).
- **Field AC is report-only/next-round**, measured under scan/count once pruning (#2679/#2806) is in.
