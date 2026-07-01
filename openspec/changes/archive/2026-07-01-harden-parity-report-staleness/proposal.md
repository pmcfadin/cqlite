# Proposal: harden-parity-report-staleness

> Milestone: Cassandra Byte-for-Byte Parity Program — CI gate hardening (follow-up to epic #974 / the
> parity-manifest lane). Issue: #1338. Routing: **design-driven** (CI process + gate tooling; no new
> oracle bytes, no parser/compaction change) → OpenSpec.
> Supersedes the mechanism described in #1338's original body (see "Corrected root cause" below).

## Why

`docs/reports/cassandra-test-parity.md` is a **derived artifact committed alongside its source**
(`test-data/cassandra-parity-manifest.yml`). `.github/workflows/cassandra-parity.yml` runs a
`cassandra-parity report ... --check` staleness guard that fails when the committed report differs from a
fresh render. On 2026-06-30 this guard went **red on `main`** and, because PR CI merges against `main`'s
tip, it red-flagged the `parity-manifest` check on **every open PR** until #1331 regenerated the report.

### Corrected root cause (semantic merge race — NOT a path-filter blind spot)

The drift was not caused by an input outside the workflow's `paths:` filter. Every commit that touches
the manifest also regenerates the report in the **same** commit (verified: the manifest-history and
report-history commit SHAs are identical — `eba7c8d7`, `56580d48`, `b150903c`, `86db6457`, `a01581dd`…).
Authors already regenerate correctly.

The drift is a **semantic merge race** on a committed derived artifact:

1. PR A (`#1308`) renders the report against its base and is green on its own `parity-manifest` check.
2. Before A merges, PRs that also change manifest scenario counts (`#1302`, `#1306`) merge to `main`.
3. A squash-merges. Git sees no textual conflict (different lines), so it keeps A's report — which was
   rendered **without** the manifest entries from `#1302`/`#1306`. The merged `main` now has a report that
   matches **neither** branch's view → stale.
4. `#1308`'s post-merge push run (`28452290329`, 14:32:53Z — the same instant `56580d48` landed) is the
   first red, and it reds the whole PR queue.

GitHub's merge button does **not** require a branch to be up to date with `main` before merging, so a
derived artifact regenerated against a stale base silently regresses on merge. A per-PR `--check` cannot
catch this — every PR is green against its own base; the staleness exists **only post-merge**.

This is the second parity-report fire in one day (#1330/#1331 was the first). The cost is high: a stale
derived artifact blocks the **entire** PR queue, not just the PR that caused it.

## What Changes

The outcome — not a specific mechanism — is fixed: **a stale committed parity report cannot persist on
`main` undetected, and correcting it MUST NOT require manually unblocking the whole PR queue.** The
concrete mechanism is the owner's decision at approval (design D1); this proposal recommends a primary
and a fallback and scopes both:

- **(Recommended, D1) Post-merge self-healing.** On push to `main`, when `--check` reports stale, the
  workflow opens an automated regeneration PR (it does NOT push directly to protected `main`) titled for
  the drift, so `main` is corrected within one cycle without a human regenerating by hand. The push-run
  staleness check stays (it is the trigger), but its red no longer requires manual queue rescue.
- **(Belt-and-suspenders, always) Local + agent-gate coverage.** Add a SKIP-aware `parity-report`
  component to `scripts/agent-gate.sh` that runs the `--check` when the tool/manifest are present, so the
  *single-PR forgetful-regen* case (the #1330 class) is caught locally before push — complementing the
  race fix, which only the post-merge path can address.
- **(Alternative, D1) Pre-merge up-to-date enforcement.** Require branches up to date before merge (a
  GitHub merge queue / branch-protection setting) for parity-touching PRs, which defeats the race by
  forcing a re-render against tip. Bulletproof but serializes merges — a poor fit for the current busy
  queue + "CI overloaded" sprint. Documented as the fallback if self-healing proves insufficient.
- **Doctrine update (same change):** record the derived-artifact + merge-race hazard and the chosen
  mechanism in `docs/development/parity-ci-tiers.md` (or the manifest doctrine page) and note it in
  `CLAUDE.md`, so the next author understands why the report can drift even when they regenerate correctly.

## Non-goals

- Changing what the report contains or how `cassandra-parity report` renders it.
- Removing the committed report / generating it only at docs-build time (a larger refactor with unknown
  downstream link/tooling dependencies) — out of scope; noted as a future option in design.
- Touching any parser, writer, compaction, or oracle-byte behavior.
- Broadening the `cassandra-parity.yml` `paths:` filter — the filter is not the cause; widening it would
  not catch a post-merge race and is explicitly rejected.

## Capabilities

### New Capabilities
- `parity-report-integrity`: Guarantees the committed `docs/reports/cassandra-test-parity.md` cannot
  persist stale on `main` undetected — a post-merge staleness detection that self-heals via an automated
  regeneration PR (not a direct push to protected `main`), plus a SKIP-aware local/agent-gate
  `parity-report` check that catches a forgotten single-PR regen before push — without requiring a human
  to manually unblock the PR queue.
