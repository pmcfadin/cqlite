# Proposal: Weight-balanced split→pod assignment via drain-hardened sub-splitting

## Milestone / theme
0.16 (milestone #12) — cqlite-trino throughput theme; the **release-closing** item. Fixes issue #2680.

## Routing
**Design-driven** (Trino connector split-planning + Flight read-path lifecycle). No SSTable decode change.

## Problem

One flight pod carries ~650m CPU vs 140–300m on the others, capping aggregate throughput at ~39 qps
(Lane-B B2 goal ≥100 qps @ 32 threads not met). The #2397/#2409 deterministic per-range primary rotation
(`sorted[floorMod(startToken, size)]`) fixed pod *participation* but not *balance*:

- Splits are **count-balanced, not weight-balanced**: `getSplitWeight()` is never overridden (both splits
  report the SPI default 1.0), and the #2409 acceptance test asserts range-**count** spread only.
- With only ~48 vnode ranges over 3 pods, whichever pod's `floorMod`-assigned ranges are heavy does 2–4×
  the CPU. Reassigning *whole* ranges cannot help when a **single** range is the heavy one — it is stuck
  on one pod.

**Constraint discovered in triage:** there is **no per-range size feed** anywhere — Sidecar
`ReplicaInfo` carries only `{start, end, replicas}`, and Flight `table_stats` is whole-table. So true
weight (bytes/partitions per range) is unavailable at plan time without building a new server feed.

## Prior attempt and why it was reverted (load-bearing)

PR #2779 shipped K-way token sub-splitting (default K=4) + span-proportional `getSplitWeight()`. It was
**reverted by PR #2791** because it shipped a **P0 (#2782): `LIMIT n` queries hang 180s** through
cqlite_flight. Root cause: with ~4× the `DoGet` streams in flight, Trino stops scheduling the remainder
once `LIMIT n` is satisfied and closes operators early — and a scan's Flight `DoGet` stream was **released
(`handle.close()`) but not explicitly cancelled/drained** on early close, so the query never finished.
Diagnostic evidence: `LIMIT 2` (early stop) hung; `LIMIT 100` (full drain, above table size) and unbounded
scans **passed**. The aggregate/count(\*) path was already K=1-exempt and unaffected. **Only the
docker-compose `Flight ↔ Trino E2E` lane caught it** (Java units all passed); the E2E red was misread and
`--auto` armed anyway.

## What we will build — sub-splitting done right

Statistical balance beats proxy balance when weights are unknown: dealing more, smaller slices round-robin
across pods evens load out **regardless of where the weight concentrates**, and it is the only approach
that can break a single heavy range across pods (the throughput ceiling). We resurrect the archived
ring-slicing design (its slice math and invariants were sound; the P0 was purely the drain), and add three
things that make it safe:

1. **Root-fix the early-close drain (prerequisite, lands first).** On early operator close the scan page
   source SHALL explicitly **cancel** the Flight `DoGet` stream (not merely release the handle), so an
   un-consumed stream never blocks — at any split count. This fixes the #2782 root cause for K=1 too.
2. **Keep the hang-prone, no-benefit shapes at K=1.** Sub-split **only** the unbounded scan / count(\*)
   workloads the AC targets. When a `LIMIT` is pushed onto the handle **or** the read is a bound-key point
   read, plan at **K=1** (parent-range granularity) — so the LIMIT shape structurally stays out of the
   multi-stream path even independent of the drain fix (defense in depth).
3. **Gate on the test that caught it.** A docker-compose E2E `LIMIT 2` (and partial-predicate `LIMIT`)
   **hang regression** is a **hard merge gate**; a red `flight-trino-e2e` blocks merge — never `--auto`
   past it.

Plus the archived design's balance mechanics: deterministic equal-span K-way slicing, weight-balanced
per-range rotation of slice primaries (within ≤1.25× of mean per-owner assigned span), span-proportional
`getSplitWeight()` (aggregate clamped), and all failover/snapshot/pruning invariants preserved at slice
granularity.

## Non-goals

- **A per-range size feed** (bytes/partitions per range) — does not exist; building it (new Flight/Sidecar
  scoping across Rust core + server) is deferred to a tracked follow-up. This change balances on
  **token-span geometry + statistical spread**, which needs no feed. If the field CPU target is not met,
  the per-range feed becomes the next-round lever.
- **Learned/observed cross-query weights** — rejected: stateful, cold-start-blind, and in tension with the
  deterministic-assignment AC.
- **Aggregate-path sub-splitting** — the finalize split occupies ONE driver; sub-splitting it only
  multiplies serialized `DoGet` round trips. Aggregate stays K=1 (its snapshot chooser evaluated at K=1).

## Doctrine impact

- No-heuristics: token-span is authoritative ring geometry, not an inferred type/behavior — clean under #28.
- The **field CPU AC (busiest pod ≤ ~1.3× median @ 32 threads; qps ceiling off ~39) is report-only /
  next-round**, measured under scan/count once split-pruning (#2679/#2806) is in; the mergeable ACs are the
  unit ones (per-owner weight ≤1.25× mean, deterministic, `getSplitWeight()` surfaced, owner set retained)
  plus the new drain + LIMIT-exemption + E2E-gate requirements.
- Update the connector README (`cqlite.sub-splits-per-range`) and the flight-trino docs page.
