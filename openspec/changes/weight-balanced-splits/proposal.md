# weight-balanced-splits

## Why

The v0.15.0 milestone soak (#2661) shows **one flight pod carrying ~650m CPU vs 140–300m** on the
others at 32 threads — the same pattern as rounds 9/10b — capping aggregate throughput at ~39 qps
(**Lane-B B2 ≥100 qps @ 32-thr not met**). #2397 (all splits pinned to ONE pod) was fixed by PR #2409
with deterministic per-range primary rotation (`sorted[floorMod(startToken, size)]`,
`CqliteFlightSplitManager.rotate()`), but that rotation is **count-balanced only**: every split
inherits Trino's default `SplitWeight.standard()` and no notion of range weight enters assignment.
Sidecar vnode token ranges cover very unequal token spans (and therefore, under Murmur3's uniform
partition placement, very unequal partition counts), so with only ~48 ranges whichever pod's
floorMod-assigned ranges are heavy does 2–4× the CPU of the others. Count-balanced ≠ weight-balanced.

- **Milestone:** 0.16 — issue #2680 (weight-balanced split→pod assignment), epic #2403 Lane-B B2.
- **Routing:** design-driven. Two candidate directions existed (weight-aware LPT bin-packing vs
  deterministic sub-splitting) with different RPC-surface implications — a real design call, hence
  OpenSpec.

## What Changes

`CqliteFlightSplitManager` deterministically **sub-divides each Sidecar read-replica token range into
K equal-token-span slices** (new config `cqlite.sub-splits-per-range`, default 4, min 1, max 64)
**before any split construction**, and rotates each range's slices across that range's own replica
owner set. Because every owner receives ~1/N of *each* range's token span, per-owner assigned weight
converges to ~1/N of the total **regardless of how unequal the per-range weights are** — no size
estimates, no new RPC. The scan ticket already carries arbitrary `tokenStart`/`tokenEnd`/`wraparound`
(`FlightTicketJson`), so the flight server needs no changes.

Additionally, `getSplitWeight()` is overridden on the connector's splits, proportional to assigned
token span, so Trino's scheduler accounts for uneven slices instead of counting all splits as equal.

All downstream consumers — scan splits, aggregate fan-out, the snapshot host chooser (#2227), and
plan-time pruning (#2679) — consume the sliced assignment from a single seam, so the existing
invariants (full ordered failover owner set #2241, snapshot chooser covers every primary #2227,
deterministic re-planning) hold at slice granularity. A fully-bound point read now prunes to exactly
one *slice* (a strictly narrower server-side range than #2679's one *range*).

## Non-goals

- **Per-range size/byte estimates and LPT bin-packing** (candidate direction 1): requires a new or
  extended per-range stats RPC (`TableStatsRequest` is whole-table today). Deferred as the follow-up
  if field spread persists after this change — intra-range *bytes-per-partition* skew is the one
  residual this design cannot see.
- **Trino-worker (coordinator-side) scheduling / address-hint changes** — refuted as a cause in
  #2680's triage; out of scope.
- **Flight server / Rust-side changes** — the scan path already honors arbitrary sub-ranges.
- **Field verification** (busiest pod ≤ ~1.3× median CPU at 32 threads, metric C10) is report-only
  in the next image round, measured under scan/count workloads now that split-pruning (#2679) keeps
  point reads from fanning out. It is an acceptance signal for the *issue*, not a CI-testable
  requirement of this change.

## Doctrine impact

None to CLAUDE.md or the website. Connector config reference gains `cqlite.sub-splits-per-range`
(documented in the connector docs as part of this change).
