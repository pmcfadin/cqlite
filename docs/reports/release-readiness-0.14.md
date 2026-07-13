# 0.14 Release Readiness Assessment

**Date:** 2026-07-08 · **Author:** flow-lead (`pmcfadin-ltmcx74`) · **Status:** DRAFT for owner review
**Current main version:** `0.13.0` (not yet bumped) · **Milestone 0.14:** 95 closed / 14 open (**87% done**)

> Assessment only — milestone re-scoping, the merge of release blockers, and the two owner-gated
> decisions (#2022 scope, #1935 CI-regen) are owner/manager calls. Nothing here is acted on unilaterally.

---

## 1. Verdict

**0.14 is close — realistically 5 items from cuttable, and only 3 need action beyond landing in-flight PRs.**
The open count (14) overstates the work: 6 of the 14 are non-blocking follow-up/tracking issues that
should move to 0.15. The true release gate is **2 P0 + 3 P1**, and **4 of those 5 already have open PRs**.

The single biggest risks are two **owner-gated decisions** that have been pending, not engineering effort.

---

## 2. Critical path (the real blockers)

| # | P | Title | State | Owner-action? |
|---|---|-------|-------|---------------|
| **#1742** | P0 | query-semantics oracle | **PR #2187 open** (peer pipeline) | No — land on green |
| **#1849** | P0 | multi-gen SELECT read-time TTL/tombstone visibility (data-loss) | **PR #2192 open** (peer pipeline) | No — land on green |
| **#2022** | P1 | Coverage Gate (90%) enforced leg | **PR #2203 open** (partial) | **YES — scope decision** |
| **#1935** | P1 | Regenerate TTL corpus without TTL | **code half MERGED (#2209); blocked-on-CI** | **YES — CI-regen go** |
| **#1644** | P1 | K5 zero-copy value extraction | **stale** (no lock/PR; `HOLD: after #1583`, now CLOSED) | **YES — reconcile/reclaim** |

### The 3 that need a decision, not just a merge
1. **#2022 — Coverage Gate scope (OWNER).** The enforced leg has *never been green*: it scores the
   **whole `cqlite-core` crate at 90%** (currently 78.3%, ~12.5k uncovered lines) while `tarpaulin.toml`
   documents a **read-path-only** scope. This is a policy call: re-scope to read-path (recommended,
   smallest correct move), set a realistic threshold + ratchet, or treat as a multi-week epic. A peer
   landed "Layer 1" via PR #2203 but the threshold/scope policy is unresolved.
2. **#1935 — CI regen (OWNER GO).** Code half merged (schemas stripped of TTL, TTL-aware asserts).
   Full closure needs `exhaustive-regeneration.yml` → v3.5 dataset asset → pin bump → fetch round-trip.
   The asset upload is **irreversible** → owner-gated. Until it runs, the 5 corpus tables' binaries still
   carry TTL (harmless — reader shadows to 0-live — but the dataset-pin AC isn't met).
3. **#1644 — reconcile.** Board shows `status:in-progress`, but there is **no claim branch and no PR**,
   and its blocker `HOLD: merge after #1583` is now moot (**#1583 CLOSED**). Either it's genuinely
   unstarted (→ reclaim as a real lane) or done-elsewhere (→ close). Needs a 5-minute reconcile.

---

## 3. NOT blockers — move to 0.15 (milestone hygiene)

These inflate the 0.14 open count from 14 → would be **8** after removal:

- **#2173, #2172, #2167, #2151, #2145** — "Follow-ups from …" issues, each **self-labeled
  non-blocking / polish / "none affect correctness."** Batched review nits from already-merged PRs.
- **#1655** — a *tracking umbrella* ("NOT a standalone sweep; rides functional PRs"), not a work item.
  → Backlog or close; it has been mis-parked in Ready repeatedly.

Recommend: re-milestone the 5 follow-ups to 0.15; de-Ready + Backlog #1655.

## 4. Non-gating but decision-worthy

- **#1979 — Node.js windows-only CI (3 chronic suites).** A *release-quality* question: do we ship 0.14
  with 3 known windows-only test failures? Not a core-library code blocker; not verifiable from the
  macOS delivery host. **Owner call whether it gates the cut.**
- **#1817 (P2), #1645 (P2)** — read-path perf (Epic E/K follow-ups). Nice-to-have, not release-gating.
  #1817 (internal FxHashMap + partition-key hoist) is implemented + lite-green, in review now.

## 5. Main / CI health

- Recent `main` runs: **10 success / 1 failure / 1 in-progress**. The failure is **Flight ↔ Trino E2E**
  (matches the pre-existing `#1979`-adjacent windows/E2E flakiness — verify it's the known flake, not a
  regression, before the cut). Core gate lanes green.
- No required status checks on `main` (contexts=[]) — the `agent-gate.sh` of record + nightly `gate.yml`
  deep-check are the standing backstops. Release cut should run a clean full gate on the release SHA.

## 6. Shipped in 0.14 (95 closed — CHANGELOG themes)

| Area | # |
|------|---|
| CI / gate infra | 15 |
| Parser / format (BTI, Index, Statistics, decode) | 14 |
| Compression (lz4/zstd/deflate/snappy, chunk) | 12 |
| CLI / output / export | 10 |
| Read-path perf (alloc, zero-copy, cache, prefetch) | 9 |
| Bindings (Python/Node) | 8 |
| TTL / tombstone correctness | 5 |
| Flight / Trino | 3 |
| Parity / fixtures | 2 |
| Hygiene / other | 17 |

Headline narrative for the release notes: **read-path correctness + performance** (point-query
routing/no-heuristics #1750, multi-gen visibility #1849, zero-copy/alloc reductions), **compression
breadth**, **CLI/output**, and **Flight/Trino user docs** (#2115).

## 7. Recommended cut checklist (proposed)

1. [ ] **OWNER:** decide #2022 coverage-gate scope; **OWNER:** give #1935 CI-regen go.
2. [ ] Land P0 PRs #2187 (#1742) + #2192 (#1849) on green (peer pipelines).
3. [ ] Reconcile #1644 (reclaim or close).
4. [ ] Run #1935 exhaustive-regeneration → v3.5 asset → pin bump → close #1935.
5. [ ] Milestone hygiene: 5 follow-ups → 0.15; #1655 → Backlog.
6. [ ] Decide #1979 windows-only gate/no-gate for the cut.
7. [ ] Confirm Flight↔Trino E2E failure is the known flake, not a regression.
8. [ ] Bump version 0.13.0 → 0.14.0; finalize CHANGELOG from the 95 closed.
9. [ ] Clean full `agent-gate.sh` on the release SHA; tag + release.

---
*Living document — regenerate as blockers land.*
